#!/usr/bin/env python3

"""Command-line interface for biorxiv_search using rich-click.

This CLI mirrors the visual/style configuration used by geNomad's `cli.py` but
is intentionally minimal: it provides a single `search` command which calls
`fetch_exact_affil` from the sibling `biorxiv_search` module.
"""

import shutil
from pathlib import Path
from typing import Optional

import rich_click as click
from rich.console import Console
import polars as pl

from search import fetch_exact_affil
import fetch as fetch_module

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"]) 
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.SHOW_METAVARS_COLUMN = False
click.rich_click.APPEND_METAVARS_HELP = True
click.rich_click.MAX_WIDTH = None
click.rich_click.STYLE_OPTIONS_TABLE_BOX = "SIMPLE"
click.rich_click.STYLE_COMMANDS_TABLE_SHOW_LINES = True
click.rich_click.STYLE_COMMANDS_TABLE_PAD_EDGE = True
click.rich_click.STYLE_COMMANDS_TABLE_BOX = "SIMPLE"

click.rich_click.COMMAND_GROUPS = {
    "biorxiv_search": [
        {"name": "Main", "commands": ["search", "fetch"]},
    ]
}


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """biorxiv_search: search biorxiv/medrxiv for exact corresponding author affiliation"""


@cli.command("search", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--server",
    type=click.Choice(["biorxiv", "medrxiv", "both"]),
    default="both",
    show_default=True,
    help="Server to query (biorxiv, medrxiv, or both).",
)
@click.option(
    "--start",
    "start_date",
    default="2013-01-01",
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    "end_date",
    default=None,
    show_default=True,
    help="End date (YYYY-MM-DD). Defaults to today.",
)
@click.option(
    "--target",
    "target_affil",
    default="Institut Pasteur",
    show_default=True,
    help="Target affiliation (exact match, case-insensitive).",
)
@click.option(
    "--pause",
    "pause_s",
    type=float,
    default=0.2,
    show_default=True,
    help="Pause between batches when polling the executor (seconds).",
)
@click.option(
    "--outdir",
    "outdir",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True, writable=True),
    default=Path('.'),
    show_default=True,
    help="Directory where output CSV(s) will be written.",
)
@click.option(
    "--cpu",
    "cpu",
    type=int,
    default=4,
    show_default=True,
    help="Number of concurrent requests to use.",
)
@click.option(
    "--max-retries",
    "max_retries",
    type=int,
    default=50,
    show_default=True,
    help="Maximum number of retries per page for transient failures.",
)
@click.option(
    "--temp-outdir",
    "temp_outdir",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=None,
    show_default=True,
    help="Directory for temporary batch files (enables resume). If not specified, a temporary directory will be created and removed on completion.",
)
def search(server: str, start_date: str, end_date: Optional[str], target_affil: str, pause_s: float, outdir: Path, cpu: int, max_retries: int, temp_outdir: Optional[Path]):
    """Search the selected server(s) and write CSV(s) of matching records.

    The CSV filenames are: {server}_{target}_{start_date}_{end_date}_extract.csv
    Dates in filenames are formatted as YYYYMMDD (e.g., 20200101).
    When both servers are queried, an additional combined CSV is written.
    
    Features:
    - Parallel page fetching with configurable concurrency
    - Resume capability via temporary batch files
    - Real-time progress tracking with article counter
    - Configuration summary displayed at start
    - Automatic retry with exponential backoff for transient errors
    """
    console = Console()
    servers = ["biorxiv", "medrxiv"] if server == "both" else [server]
    dfs = []

    outdir = outdir.expanduser()
    
    # Set up temp directory for batch files
    if temp_outdir is None:
        tempdir = outdir / "temp_batches"
    else:
        tempdir = temp_outdir.expanduser()

    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        console.print(f"[red]Could not create output directory '{outdir}': {e}")
        raise click.ClickException(f"Could not create output directory '{outdir}': {e}")

    for s in servers:
        console.print(f"Querying [bold]{s}[/] from {start_date} to {end_date or 'today'}...", style="green")
        
        # Create server-specific temp directory if using temp_outdir
        server_tempdir = tempdir / s if tempdir else None
        
        df = fetch_exact_affil(
            s, 
            start_date=start_date, 
            end_date=end_date, 
            target_affil=target_affil, 
            pause_s=pause_s,
            temp_outdir=server_tempdir,
            concurrency=cpu,
            max_retries=max_retries
        )
        
        # Build filename with date range
        safe_target = target_affil.replace(" ", "_")
        # Use actual date instead of "today"
        from datetime import date
        actual_end_date = end_date or str(date.today())
        safe_end_date = actual_end_date.replace("-", "")
        safe_start_date = start_date.replace("-", "")
        prefix = s
        name = f"{prefix}_{safe_target}_{safe_start_date}_{safe_end_date}_extract.csv"
        out_path = Path(outdir) / name
        df.write_csv(out_path)
        console.print(f"Wrote [bold]{len(df)}[/] records to [cyan]{out_path}[/]\n")
        dfs.append(df)

    if len(dfs) > 1:
        combined = pl.concat(dfs)
        
        # Build combined filename with date range
        safe_target = target_affil.replace(" ", "_")
        # Use actual date instead of "today"
        from datetime import date
        actual_end_date = end_date or str(date.today())
        safe_end_date = actual_end_date.replace("-", "")
        safe_start_date = start_date.replace("-", "")
        prefix = "both"
        combined_name = f"{prefix}_{safe_target}_{safe_start_date}_{safe_end_date}_extract.csv"
        combined_path = Path(outdir) / combined_name
        combined.write_csv(combined_path)
        console.print(f"Wrote combined [bold]{len(combined)}[/] records to [cyan]{combined_path}[/]")

    # remove the temp dir if it was created by us
    all_download = False

    if server == "both":
        biorxiv_file = outdir / f"biorxiv_{target_affil.replace(' ', '_')}_{start_date.replace('-', '')}_{(end_date or str(date.today())).replace('-', '')}_extract.csv"
        medrxiv_file = outdir / f"medrxiv_{target_affil.replace(' ', '_')}_{start_date.replace('-', '')}_{(end_date or str(date.today())).replace('-', '')}_extract.csv"
        if biorxiv_file.exists() and medrxiv_file.exists():
            all_download = True
    else:
        single_file = outdir / f"{server}_{target_affil.replace(' ', '_')}_{start_date.replace('-', '')}_{(end_date or str(date.today())).replace('-', '')}_extract.csv"
        if single_file.exists():
            all_download = True

    if tempdir and all_download:
        try:
            shutil.rmtree(tempdir)
        except OSError as e:
            console.print(f"[yellow]Warning: failed to remove temp dir {tempdir}: {e}[/]")

@cli.command("fetch", context_settings=CONTEXT_SETTINGS)
@click.option("--start", default="2013-01-01", show_default=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", default=None, show_default=True, help="End date (YYYY-MM-DD). Defaults to today.")
@click.option("--outdir", "outdir", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=Path("out_parquet"), show_default=True, help="Directory to write Parquet output files.")
@click.option("--cpu", type=int, default=2, show_default=True, help="Number of concurrent page requests.")
@click.option("--server", type=click.Choice(["biorxiv", "medrxiv", "both"]), default="both", show_default=True, help="Server to query (biorxiv, medrxiv, or both).")
def fetch_parquet(server: str, start: str, end: Optional[str], outdir: Path, cpu: int):
    """Fetch all records (no affiliation filtering) and write Parquet outputs.
    
    Features:
    - Fetches all available records between dates (no filtering)
    - Parallel page fetching for faster downloads
    - Resume capability via temp_batches/ directory
    - Real-time progress tracking
    - Configuration summary at start
    - Drops abstract field to reduce file size
    - Deduplicates by DOI, keeping highest version
    """
    console = Console()
    servers = ["biorxiv", "medrxiv"] if server == "both" else [server]
    outdir = outdir.expanduser()
    tempdir = outdir / "temp_batches"

    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        console.print(f"[red]Could not create output directory '{outdir}': {e}")
        raise click.ClickException(f"Could not create output directory '{outdir}': {e}")

    if server == "both":
        console.print(f"Querying [bold]biorxiv and medrxiv[/] from {start} to {end or 'today'}...", style="green")
    else:   
        console.print(f"Querying [bold]{server}[/] from {start} to {end or 'today'}...", style="green")

    dfs = fetch_module.fetch_servers(servers, start, end, 0.2, cpu, tempdir)
    fetch_module.write_parquet_tables(dfs, Path(outdir), prefix=None)

    # remove the temp dir if it was created by us
    all_download = False

    if server == "both":
        biorxiv_file = outdir / "biorxiv.parquet"
        medrxiv_file = outdir / "medrxiv.parquet"
        if biorxiv_file.exists() and medrxiv_file.exists():
            all_download = True
    else:
        single_file = outdir / f"{server}.parquet"
        if single_file.exists():
            all_download = True

    if tempdir and all_download:
        try:
            shutil.rmtree(tempdir)
        except OSError as e:
            console.print(f"[yellow]Warning: failed to remove temp dir {tempdir}: {e}[/]")


if __name__ == "__main__":
    cli()
