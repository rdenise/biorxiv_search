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
import parse_jats as parse_jats_module

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
        {"name": "Main", "commands": ["search", "fetch-parquet", "parse-jats"]},
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
    help="Pause between API requests in seconds.",
)
@click.option(
    "--outdir",
    "outdir",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True, writable=True),
    default=Path('.'),
    show_default=True,
    help="Directory where output CSV(s) will be written.",
)
def search(server: str, start_date: str, end_date: Optional[str], target_affil: str, pause_s: float, outdir: Path):
    """Search the selected server(s) and write CSV(s) of matching records.

    The CSV filenames are: <prefix>_<target>_exact.csv where <prefix> is either
    the server name or the user-supplied --prefix. When both servers are
    queried, an additional combined CSV is written.
    """
    console = Console()
    servers = ["biorxiv", "medrxiv"] if server == "both" else [server]
    dfs = []

    outdir = outdir.expanduser()
    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        console.print(f"[red]Could not create output directory '{outdir}': {e}")
        raise click.ClickException(f"Could not create output directory '{outdir}': {e}")

    for s in servers:
        console.print(f"Querying [bold]{s}[/] from {start_date} to {end_date or 'today'}...", style="green")
        df = fetch_exact_affil(s, start_date=start_date, end_date=end_date, target_affil=target_affil, pause_s=pause_s)
        safe_target = target_affil.replace(" ", "_")
        prefix = s
        name = f"{prefix}_{safe_target}_extract.csv"
        out_path = Path(outdir) / name
        df.write_csv(out_path)
        console.print(f"Wrote [bold]{len(df)}[/] records to [cyan]{out_path}[/]")
        dfs.append(df)

    if len(dfs) > 1:
        combined = pl.concat(dfs)
        prefix = "both"
        combined_name = f"{prefix}_{target_affil.replace(' ', '_')}_extract.csv"
        combined_path = Path(outdir) / combined_name
        combined.write_csv(combined_path)
        console.print(f"Wrote combined [bold]{len(combined)}[/] records to [cyan]{combined_path}[/]")


@cli.command("fetch", context_settings=CONTEXT_SETTINGS)
@click.option("--start", default="2013-01-01", show_default=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", default=None, show_default=True, help="End date (YYYY-MM-DD)")
@click.option("--outdir", "outdir", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=Path("out_parquet"), show_default=True)
@click.option("--concurrency", type=int, default=2, show_default=True)
@click.option("--server", type=click.Choice(["biorxiv", "medrxiv", "both"]), default="both", show_default=True, help="Server to query (biorxiv, medrxiv, or both).")
def fetch_parquet(server: str, start: str, end: Optional[str], outdir: Path, concurrency: int):
    """Fetch all records between two dates for biorxiv and medrxiv and write Parquet outputs (drops abstract)."""
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

    dfs = fetch_module.fetch_servers(servers, start, end, 0.2, concurrency, tempdir)
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


@cli.command("parse-jats", context_settings=CONTEXT_SETTINGS)
@click.option("--parquet", "parquet_path", required=True, type=click.Path(path_type=Path, file_okay=True, dir_okay=False))
@click.option("--target", "target_affil", default="Institut Pasteur", show_default=True)
@click.option("--outdir", "outdir", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=Path("out_jats"), show_default=True)
@click.option("--concurrency", type=int, default=8, show_default=True)
def parse_jats(parquet_path: Path, target_affil: str, outdir: Path, concurrency: int):
    """Parse JATS XMLs for entries in a Parquet file and create augmented tables."""
    parse_jats_module.analyze_parquet_jats(Path(parquet_path), Path(outdir), target_affil=target_affil, concurrency=concurrency)


if __name__ == "__main__":
    cli()
