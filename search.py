#!/usr/bin/env python3

"""Core functions for biorxiv_search.

This module exposes `fetch_exact_affil` which queries the biorxiv/medrxiv
API and returns a polars.DataFrame of matching records. It intentionally does
not contain CLI wiring; create a separate `cli.py` to provide the command
line interface.

The implementation now supports a temporary output directory with per-page
parquet files so a long-running crawl can be resumed. It also parallelizes
page fetches using a ThreadPoolExecutor and uses retry/backoff for transient
errors (e.g. HTTP 421, 500).
"""

from __future__ import annotations

import time
import tempfile
import shutil
from datetime import date
from pathlib import Path
from collections import deque
from typing import Optional, Tuple

import concurrent.futures as futures
import requests
import polars as pl
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TaskProgressColumn
from rich.panel import Panel
from rich.table import Table


def fetch_exact_affil(
    server: str,
    start_date: str = "2013-01-01",
    end_date: Optional[str] = None,
    target_affil: str = "Institut Pasteur",
    pause_s: float = 0.2,
    temp_outdir: Optional[Path] = None,
    concurrency: int = 4,
    max_retries: int = 50,
) -> pl.DataFrame:
    """Return records from a biorxiv/medrxiv server where
    `author_corresponding_institution` equals ``target_affil``.

    The function will create a temporary directory (unless `temp_outdir` is
    provided) and write per-page parquet files so the crawl can be resumed if
    interrupted. Pages are fetched in parallel (I/O bound) using a
    ThreadPoolExecutor.

    Parameters
    ----------
    server
        Either "biorxiv" or "medrxiv".
    start_date, end_date
        Date window (YYYY-MM-DD). If ``end_date`` is None, today is used.
    target_affil
        Target affiliation to match exactly (case-insensitive).
    pause_s
        Pause between batches when polling the executor.
    temp_outdir
        Optional path to store temporary per-page parquet files. If None,
        a temporary dir will be created and removed on successful completion.
    concurrency
        Number of worker threads for fetching pages.
    max_retries
        Number of retries per page for transient failures.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the matched records (may be empty).
    """

    console = Console()
    if end_date is None:
        end_date = str(date.today())

    # Print summary box with run configuration
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Parameter", style="cyan", no_wrap=True)
    table.add_column("Value", style="yellow")
    
    table.add_row("Server", server)
    table.add_row("Start Date", start_date)
    table.add_row("End Date", end_date)
    table.add_row("Target Affiliation", target_affil)
    table.add_row("Concurrency", str(concurrency))
    table.add_row("Max Retries", str(max_retries))
    table.add_row("Pause (seconds)", str(pause_s))
    table.add_row("Temp Directory", str(temp_outdir) if temp_outdir else "Auto (temporary)")
    
    console.print(Panel(table, title="[bold blue]Search Configuration[/bold blue]", border_style="blue"))

    base = f"https://api.biorxiv.org/details/{server}"

    # create temp dir if not provided
    created_temp = False
    if temp_outdir is None:
        tmp = tempfile.mkdtemp(prefix=f"{server}_batches_")
        temp_outdir = Path(tmp)
        created_temp = True
    else:
        temp_outdir = Path(temp_outdir)
        temp_outdir.mkdir(parents=True, exist_ok=True)

    # inspect existing batch files to support resuming
    existing_pattern = f"{server}_{start_date}_*.parquet"
    existing_files = sorted(temp_outdir.glob(existing_pattern))
    completed = False
    cursor = 0
    fetch_queue: deque[int] = deque()

    if existing_files:
        def _cursor_from_path(p: Path) -> int:
            try:
                return int(p.stem.split("_")[-1])
            except (ValueError, IndexError):
                return -1

        cursors = sorted([_cursor_from_path(p) for p in existing_files if _cursor_from_path(p) >= 0])
        max_cursor = max(cursors) if cursors else -1
        last_files = [p for p in existing_files if _cursor_from_path(p) == max_cursor]
        last_path = last_files[-1]
        try:
            last_df = pl.read_parquet(last_path)
            last_count = last_df.height
        except (OSError, ValueError, RuntimeError):
            last_count = -1

        expected = list(range(0, max_cursor + 1, 100)) if max_cursor >= 0 else []
        existing_set = set(cursors)
        missing = [c for c in expected if c not in existing_set]

        if 0 <= last_count < 100 and not missing:
            console.print(f"[green]Found existing batches; last batch at cursor {max_cursor} has {last_count} records (<100) — assuming completed.[/]")
            completed = True
        else:
            if missing:
                console.print(f"[yellow]Found missing pages up to cursor {max_cursor}: {missing}. Will fetch missing pages before resuming.[/]")
                for c in missing:
                    fetch_queue.append(c)
            cursor = (max_cursor + 100) if max_cursor >= 0 else 0
            # Calculate existing articles count
            console.print(f"[yellow]Resuming fetch for {server} from cursor {cursor} (after max existing {max_cursor}).[/]")

    # If already completed, skip fetching
    if not completed:
        wait = 2.0

        def _fetch_page(page_cursor: int) -> Tuple[int, list]:
            url = f"{base}/{start_date}/{end_date}/{page_cursor}"
            for _ in range(max_retries):
                try:
                    r = requests.get(url, timeout=30)
                    # treat 421 and 500 and 503 as transient
                    if getattr(r, "status_code", None) in (421, 500, 503):
                        time.sleep(wait)
                        continue
                    r.raise_for_status()
                    data = r.json()
                    return page_cursor, data.get("collection", [])
                except requests.exceptions.HTTPError as e:
                    resp = getattr(e, "response", None)
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

                        # normalize funder if necessary
                        try:
                            for c in collection:
                                funder = c.get("funder")
                                if isinstance(funder, list):
                                    c["funder"] = funder[0]["name"]
                            batch_df = pl.DataFrame(collection)
                        except (TypeError, ValueError):
                            collection = [dict(x) for x in collection]
                            for c in collection:
                                funder = c.get("funder")
                                if isinstance(funder, list):
                                    c["funder"] = funder[0]["name"]
                            batch_df = pl.DataFrame(collection)

                        # ensure source_server present
                        if "source_server" not in batch_df.columns:
                            batch_df = batch_df.with_columns(source_server=pl.lit(server))

                        # write full page to temp dir
                        temp_path = temp_outdir / f"{server}_{start_date}_{cur}.parquet"
                        batch_df.write_parquet(temp_path)
                        
                        # Update progress
                        files_completed += 1
                        progress.update(task, advance=1, articles=files_completed * 100)

                        if len(collection) < 100:
                            final_seen = True

                        if not final_seen:
                            if fetch_queue:
                                c = fetch_queue.popleft()
                                pending[ex.submit(_fetch_page, c)] = c
                            else:
                                pending[ex.submit(_fetch_page, next_cursor)] = next_cursor
                                next_cursor += 100

                    time.sleep(pause_s)
    else:
        console.print(f"[blue]Skipping fetch; using existing batch files in {temp_outdir}[/]")

    # combine batch files and filter by affiliation
    files = sorted(temp_outdir.glob(f"{server}_{start_date}_*.parquet"))
    if not files:
        if created_temp:
            try:
                shutil.rmtree(temp_outdir)
            except OSError:
                pass
        return pl.DataFrame([])

    parts = [pl.read_parquet(p) for p in files]
    combined = pl.concat(parts, how="vertical")

    # cast version to integer where possible and fill nulls
    if "version" in combined.columns:
        try:
            combined = combined.with_columns(pl.col("version").cast(pl.Int64).fill_null(0))
        except (TypeError, ValueError):
            pass

    # filter by affiliation case-insensitive
    target = target_affil.strip().lower()
    if "author_corresponding_institution" in combined.columns:
        hits_df = combined.filter(pl.col("author_corresponding_institution").str.to_lowercase().str.contains(target))
    else:
        hits_df = pl.DataFrame([])

    # deduplicate by DOI keeping highest version
    if "doi" in hits_df.columns:
        hits_df = hits_df.sort(["doi", "version"], descending=[False, True])
        hits_df = hits_df.unique(subset=["doi"], keep="first")

    # add DOI URL if present and drop abstract
    if hits_df.height > 0 and "doi" in hits_df.columns:
        hits_df = hits_df.with_columns(doiURL="https://doi.org/" + pl.col("doi"))
    if "abstract" in hits_df.columns:
        hits_df = hits_df.drop("abstract")

    # cleanup temporary dir if we created it
    if created_temp:
        try:
            shutil.rmtree(temp_outdir)
        except OSError:
            console.print(f"[yellow]Warning: failed to remove temp dir {temp_outdir}[/]")

    if hits_df.height == 0:
        console.print(f"[red]No matching records found for affiliation '{target_affil}' on {server}.[/]")

    return hits_df
