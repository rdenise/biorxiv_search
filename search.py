#!/usr/bin/env python3

"""Core functions for biorxiv_search.

This module exposes `fetch_exact_affil` which queries the biorxiv/medrxiv
API and returns a polars.DataFrame of matching records. It intentionally does
not contain CLI wiring; create a separate `cli.py` to provide the command
 line interface.
"""

import time
from datetime import date
from typing import Optional

import requests
import polars as pl
from rich.console import Console


def fetch_exact_affil(
    server: str,
    start_date: str = "2013-01-01",
    end_date: Optional[str] = None,
    target_affil: str = "Institut pasteur",
    pause_s: float = 0.2,
) -> pl.DataFrame:
    """Return records from a biorxiv/medrxiv server where
    `author_corresponding_institution` equals ``target_affil``.

    Comparison is case-insensitive and trims surrounding whitespace.

    Parameters
    ----------
    server
        Either "biorxiv" or "medrxiv".
    start_date, end_date
        Date window (YYYY-MM-DD). If ``end_date`` is None, today is used.
    target_affil
        Target affiliation to match exactly (case-insensitive).
    pause_s
        Pause between paginated API requests to be polite to the server.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the matched records (may be empty).
    """

    if end_date is None:
        end_date = str(date.today())

    base = f"https://api.biorxiv.org/details/{server}"
    cursor = 0
    hits = []
    console = Console()

    # retry settings for transient request failures
    max_retries = 10
    backoff_base = 1.0  # seconds, exponential backoff multiplier

    while True:
        url = f"{base}/{start_date}/{end_date}/{cursor}"

        # attempt the request with retries for transient errors (including HTTP 421)
        for attempt in range(max_retries):
            try:
                r = requests.get(url, timeout=30)
                # treat explicit 421 as transient
                if r.status_code == 421:
                    raise requests.exceptions.HTTPError(
                        f"421 Misdirected Request for url: {url}", response=r
                    )
                r.raise_for_status()
                break  # success -> exit retry loop
            except requests.exceptions.HTTPError as e:
                resp = getattr(e, "response", None)
                if resp is not None and resp.status_code == 421:
                    wait = backoff_base * (2 ** attempt)
                    console.print(
                        f"[yellow]Received 421 Misdirected Request for {url}. "
                        f"Retrying in {wait:.1f}s (attempt {attempt+1}/{max_retries})[/]"
                    )
                    time.sleep(wait)
                    continue
                # non-retriable HTTP error -> re-raise
                raise
            except requests.exceptions.RequestException as e:
                # connection/timeout/etc. -> retry
                wait = backoff_base * (2 ** attempt)
                console.print(
                    f"[yellow]Request failed ({e}). Retrying in {wait:.1f}s "
                    f"(attempt {attempt+1}/{max_retries})[/]"
                )
                time.sleep(wait)
                continue
        else:
            # exhausted retries
            console.print(
                f"[red]Failed to fetch {url} after {max_retries} attempts.[/]"
            )
            raise requests.exceptions.RequestException(
                f"Failed to fetch {url} after {max_retries} attempts"
            )

        data = r.json()
        batch = data.get("collection", [])
        if not batch:
            break

        for rec in batch:
            aff = rec.get("author_corresponding_institution")
            if aff is not None and aff.strip().lower() == target_affil.strip().lower():
                rec["source_server"] = server
                hits.append(rec)

        # pagination: 100 enregistrements par page
        if len(batch) < 100:
            break
        cursor += 100
        time.sleep(pause_s)  # be polite to the API

    # If no hits were found, print a message (rich-styled) and return an empty DataFrame
    if not hits:
        Console().print(
            f"[red]No matching records found for affiliation '{target_affil}' on {server}.[/]",
            style="red",
        )
        cols = {
            "title": [],
            "authors": [],
            "author_corresponding": [],
            "author_corresponding_institution": [],
            "doi": [],
            "date": [],
            "version": [],
            "type": [],
            "license": [],
            "category": [],
            "jatsxml": [],
            "funder": [],
            "published": [],
            "server": [],
            "source_server": [],
            "doiURL": [],
        }

        empty_df = pl.DataFrame(cols)
        return empty_df

    hits_df = pl.DataFrame(hits)

    # add DOI URL column if doi is present, and drop abstract when available
    if "doi" in hits_df.columns:
        hits_df = hits_df.with_columns(
            doiURL= "https://doi.org/" + pl.col("doi")
        )
    if "abstract" in hits_df.columns:
        hits_df = hits_df.drop("abstract")

    return hits_df
