"""Parallel fetching utilities for biorxiv_search.

This module provides helpers to run the existing `fetch_exact_affil` across
`biorxiv` and `medrxiv` in parallel, merge results, drop the `abstract` column
and write the output as Parquet files.

Usage example:

    python -m fetch --target "Institut pasteur" --outdir ./out

"""

from __future__ import annotations

import time
import concurrent.futures as futures
import tempfile
import shutil
from pathlib import Path
from typing import List, Optional, Tuple
from collections import deque

import requests
import polars as pl
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TaskProgressColumn
from rich.panel import Panel
from rich.table import Table

console = Console()

DEFAULT_SERVERS = ["biorxiv", "medrxiv"]


def fetch_servers(
    servers: Optional[List[str]] = None,
    start_date: str = "2013-01-01",
    end_date: Optional[str] = None,
    pause_s: float = 0.2,
    concurrency: int = 2,
    temp_outdir: Optional[Path] = None,
) -> List[pl.DataFrame]:
    """Run `fetch_exact_affil` for multiple servers in parallel and return list of DataFrames.

    This function uses a ThreadPoolExecutor since the workload is I/O bound (HTTP requests).
    """
    # Process servers sequentially. Parallelism is handled per-API-page inside fetch_all_records.
    servers = servers or DEFAULT_SERVERS
    results: List[pl.DataFrame] = []

    for s in servers:
        console.print(f"Starting fetch for {s} {start_date}..{end_date or 'today'}")
        df = fetch_all_records(
            s,
            start_date=start_date,
            end_date=end_date,
            pause_s=pause_s,
            max_retries=50,
            temp_outdir=temp_outdir,
            concurrency=concurrency,
        )
        console.print(f"Finished fetch for {s}, got {len(df)} records\n")
        results.append(df)

    return results


def fetch_all_records(
    server: str,
    start_date: str = "2013-01-01",
    end_date: Optional[str] = None,
    pause_s: float = 0.2,
    max_retries: int = 50,
    temp_outdir: Optional[Path] = None,
    concurrency: int = 4,
) -> pl.DataFrame:
    """Fetch all records from the API for a server between start_date and end_date.

    This mirrors the retry/backoff logic used in `search.fetch_exact_affil` but
    returns all records instead of filtering by affiliation.
    """
    if end_date is None:
        from datetime import date

        end_date = str(date.today())

    # Print summary box with run configuration
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Parameter", style="cyan", no_wrap=True)
    table.add_column("Value", style="yellow")
    
    table.add_row("Server", server)
    table.add_row("Start Date", start_date)
    table.add_row("End Date", end_date)
    table.add_row("Concurrency", str(concurrency))
    table.add_row("Max Retries", str(max_retries))
    table.add_row("Pause (seconds)", str(pause_s))
    table.add_row("Temp Directory", str(temp_outdir) if temp_outdir else "Auto (temporary)")
    
    console.print(Panel(table, title="[bold blue]Fetch Configuration[/bold blue]", border_style="blue"))

    base = f"https://api.biorxiv.org/details/{server}"
    cursor = 0

    # reuse module-level console
    wait = 2.0

    # create temp dir if not provided
    if temp_outdir is None:
        tmp = tempfile.mkdtemp(prefix=f"{server}_batches_")
        temp_outdir = Path(tmp)
    else:
        temp_outdir = Path(temp_outdir)
        temp_outdir.mkdir(parents=True, exist_ok=True)

    # inspect existing batch files to support resuming
    existing_pattern = f"{server}_{start_date}_*.parquet"
    existing_files = sorted(temp_outdir.glob(existing_pattern))
    completed = False
    # queue of cursors that must be fetched (missing pages below max_cursor)
    fetch_queue: deque[int] = deque()
    if existing_files:
        # helper to extract cursor from filename
        def _cursor_from_path(p: Path) -> int:
            try:
                return int(p.stem.split("_")[-1])
            except (ValueError, IndexError):
                return -1

        cursors = sorted([_cursor_from_path(p) for p in existing_files if _cursor_from_path(p) >= 0])
        max_cursor = max(cursors) if cursors else -1
        # find the file with the max cursor
        last_files = [p for p in existing_files if _cursor_from_path(p) == max_cursor]
        last_path = last_files[-1]
        try:
            last_df = pl.read_parquet(last_path)
            last_count = last_df.height
        except (OSError, ValueError, RuntimeError):
            last_count = -1

        # determine expected cursors up to max_cursor and check for missing pages
        expected = list(range(0, max_cursor + 1, 100)) if max_cursor >= 0 else []
        existing_set = set(cursors)
        missing = [c for c in expected if c not in existing_set]

        if 0 <= last_count < 100 and not missing:
            console.print(f"[green]Found existing batches; last batch at cursor {max_cursor} has {last_count} records (<100) — assuming completed.[/]")
            completed = True
        else:
            # enqueue missing pages below max_cursor first
            if missing:
                console.print(f"[yellow]Found missing pages up to cursor {max_cursor}: {missing}. Will fetch missing pages before resuming.[/]")
                for c in missing:
                    fetch_queue.append(c)

            # after missing pages we'll continue at max_cursor+100
            cursor = (max_cursor + 100) if max_cursor >= 0 else 0
            # Calculate existing articles count
            console.print(f"[yellow]Resuming fetch for {server} from cursor {cursor} (after max existing {max_cursor}).[/]")

    # If we already found a completed run (last batch < 100), skip fetching
    if not completed:
        # We'll parallelize fetching of pages using a ThreadPoolExecutor. The API is paged by cursor offsets (0,100,200,...)
        # We'll submit windows of `concurrency` pages and write each page as a parquet when it completes.

        def _fetch_page(cursor: int) -> Tuple[int, list]:
            url = f"{base}/{start_date}/{end_date}/{cursor}"
            for _ in range(max_retries):
                try:
                    r = requests.get(url, timeout=30)
                    # retry on 421 (Misdirected Request) and 500 (Internal Server Error) and 503 (Service Unavailable)
                    if getattr(r, "status_code", None) in (421, 500, 503):
                        time.sleep(wait)
                        continue
                    r.raise_for_status()
                    data = r.json()
                    return cursor, data.get("collection", [])
                except requests.exceptions.HTTPError as e:
                    resp = getattr(e, "response", None)
                    # treat 421 and 500 and 503 as transient and retry
                    if resp is not None and getattr(resp, "status_code", None) in (421, 500, 503):
                        time.sleep(wait)
                        continue
                    raise
                except requests.exceptions.RequestException:
                    time.sleep(wait)
                    continue
            raise requests.exceptions.RequestException(f"Failed to fetch {url} after {max_retries} attempts")

        next_cursor = cursor
        final_seen = False
        # Initialize with existing files count for resume support
        files_completed = len(existing_files) if existing_files else 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TaskProgressColumn(),
            TextColumn("~{task.fields[articles]:,} articles done"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"[cyan]Fetching {server}...",
                total=None,
                articles=0
            )
            
            with futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
                pending: dict = {}
                # prime the executor: prefer any missing cursors in fetch_queue, otherwise use next_cursor
                for _ in range(concurrency):
                    if fetch_queue:
                        c = fetch_queue.popleft()
                        pending[ex.submit(_fetch_page, c)] = c
                    else:
                        pending[ex.submit(_fetch_page, next_cursor)] = next_cursor
                        next_cursor += 100

                while pending:
                    done, _ = futures.wait(pending.keys(), return_when=futures.FIRST_COMPLETED)
                    for fut in list(done):
                        page_cursor = pending.pop(fut)
                        exc = fut.exception()
                        if exc is not None:
                            console.print(f"[red]Error fetching page {page_cursor}: {exc}[/]")
                            raise exc
                        cur, collection = fut.result()

                        # Skip if collection is empty (we've gone past the end)
                        if len(collection) == 0:
                            console.print(f"[blue]Page {cur} is empty, skipping (end of data reached).[/]")
                            if not final_seen:
                                final_seen = True
                            continue

                        # normalize and write the page
                        try:
                            for c in collection:
                                funder = c.get("funder")
                                if isinstance(funder, list):
                                    c["funder"] = funder[0]['name']

                            batch_df = pl.DataFrame(collection)
                        except (TypeError, ValueError):
                            collection = [dict(x) for x in collection]

                            for c in collection:
                                funder = c.get("funder")
                                if isinstance(funder, list):
                                    c["funder"] = funder[0]['name']

                            batch_df = pl.DataFrame(collection)

                        if "abstract" in batch_df.columns:
                            batch_df = batch_df.drop("abstract")
                        if "source_server" not in batch_df.columns:
                            batch_df = batch_df.with_columns(source_server=pl.lit(server))

                        temp_path = temp_outdir / f"{server}_{start_date}_{cur}.parquet"
                        batch_df.write_parquet(temp_path)
                        
                        # Update progress
                        files_completed += 1
                        progress.update(task, advance=1, articles=files_completed * 100)

                        if len(collection) < 100:
                            final_seen = True

                        if not final_seen:
                            # prefer any queued missing cursors
                            if fetch_queue:
                                c = fetch_queue.popleft()
                                pending[ex.submit(_fetch_page, c)] = c
                            else:
                                pending[ex.submit(_fetch_page, next_cursor)] = next_cursor
                                next_cursor += 100

                    time.sleep(pause_s)
    else:
        console.print(f"[blue]Skipping fetch; using existing batch files in {temp_outdir}[/]")

    # After fetching all batches, concatenate temporary files and deduplicate by DOI
    files = sorted(temp_outdir.glob(f"{server}_{start_date}_*.parquet"))
    
    if not files:
        # return empty DataFrame
        return pl.DataFrame([])

    parts = [pl.read_parquet(p) for p in files]
    combined = pl.concat(parts, how="vertical")

    # cast version to integer where possible and fill nulls
    if "version" in combined.columns:
        try:
            combined = combined.with_columns(pl.col("version").cast(pl.Int64).fill_null(0))
        except (TypeError, ValueError):
            # leave as-is if cast fails
            pass

    # deduplicate by DOI keeping highest version
    if "doi" in combined.columns:
        combined = combined.sort(["doi", "version"], descending=[False, True])
        combined = combined.unique(subset=["doi"], keep="first")

    return combined


def write_parquet_tables(dataframes: List[pl.DataFrame], outdir: Path, created_temp: bool, temp_outdir: Optional[Path] = None, prefix: Optional[str] = None):
    """Write per-server parquet files (and a combined parquet) while dropping `abstract` column if present."""
    outdir.mkdir(parents=True, exist_ok=True)

    dataframes = [df for df in dataframes if df.height > 0]

    for df in dataframes:
        # try to derive server name from the `source_server` column if present
        server = None
        if "source_server" in df.columns and df.height > 0:
            server = df["source_server"][0]
        server = str(server) if server is not None else "server"
        safe_target = prefix or server
        out_path = outdir / f"{safe_target}.parquet"
        if "abstract" in df.columns:
            df = df.drop("abstract")
        df.write_parquet(out_path)
        console.print(f"Wrote {len(df)} rows to {out_path}")

    # combined
    if len(dataframes) > 1:
        combined = pl.concat(dataframes)
        if "abstract" in combined.columns:
            combined = combined.drop("abstract")
        combined_path = outdir / f"{prefix or 'combined'}.parquet"
        combined.write_parquet(combined_path)
        console.print(f"Wrote combined {len(combined)} rows to {combined_path}")
    else:
        console.print("[yellow]Note: Only one server returned results; no combined file created.[/]")

    # cleanup temporary dir if we created it
    if created_temp:
        try:
            shutil.rmtree(temp_outdir)
        except OSError:
            console.print(f"[yellow]Warning: failed to remove temp dir {temp_outdir}[/]")