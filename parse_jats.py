"""Parse JATS XMLs referenced by the fetched metadata and create augmented tables.

This module expects a parquet table produced by `fetch.py` with at least the
columns: `title`, `authors`, `author_corresponding`, `author_corresponding_institution`,
`doi`, `jatsxml`, `source_server`, etc.

It will concurrently fetch each `jatsxml` URL, parse author order and
affiliations, and produce two Parquet outputs:

- `primary_affil.parquet`: rows where the target affiliation appears in the
  first two authors or last two authors (flag columns added)
- `other_affil.parquet`: rows where the target affiliation appears in any
  author but not among the first two or last two

"""

from __future__ import annotations

import concurrent.futures as futures
from pathlib import Path

import requests
import polars as pl
from rich.console import Console

try:
    from lxml import etree  # type: ignore
except ImportError:
    # fallback to the stdlib parser if lxml is not available
    import xml.etree.ElementTree as etree  # type: ignore

console = Console()


def parse_authors_and_affiliations_from_jats(jats_text: str) -> list[dict]:
    """Return a list of authors with their names and affiliations extracted from JATS XML text.

    The return format is a list of dicts: {"name": str, "affiliations": [str,...]}
    """
    # try to handle both lxml and ElementTree: normalize to an element tree
    if hasattr(etree, "fromstring"):
        root = etree.fromstring(jats_text.encode("utf-8"))
    else:
        root = etree.ElementTree(etree.fromstring(jats_text.encode("utf-8"))).getroot()

    # We'll search for contrib elements with attribute contrib-type='author'
    authors = []
    # Simple XPath-like search: find all contrib elements and filter
    for contrib in root.findall(".//contrib"):
        if contrib.get("contrib-type") != "author":
            continue

        # name handling
        name_el = contrib.find("string-name") or contrib.find("name")
        if name_el is None:
            name_text = (contrib.text or "").strip()
        else:
            surname = name_el.findtext("surname") or ""
            given = name_el.findtext("given-names") or ""
            name_text = (given + " " + surname).strip() if (given or surname) else (name_el.text or "").strip()

        affils = []
        # xref-style affiliations
        for x in contrib.findall("xref"):
            if x.get("ref-type") == "aff":
                rid = x.get("rid")
                if rid:
                    aff_el = root.findall(f".//aff[@id='{rid}']")
                    if aff_el:
                        # gather text
                        affs = []
                        for part in aff_el[0].iter():
                            if part.text:
                                affs.append(part.text.strip())
                        affils.append(" ".join([a for a in affs if a]))

        # fallback: direct <aff> children
        if not affils:
            for aff in contrib.findall("aff"):
                parts = []
                for p in aff.iter():
                    if p.text:
                        parts.append(p.text.strip())
                if parts:
                    affils.append(" ".join([p for p in parts if p]))

        authors.append({"name": name_text, "affiliations": affils})
    return authors


def analyze_parquet_jats(
    parquet_path: Path,
    outdir: Path,
    target_affil: str = "Institut Pasteur",
    concurrency: int = 8,
):
    outdir.mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(parquet_path)

    primary_rows = []
    other_rows = []

    def analyze_row(row: dict):
        # row is a mapping containing at least `jatsxml` and metadata
        jats_url = row.get("jatsxml")
        if not jats_url or jats_url in ("NA", "", None):
            return None
        try:
            r = requests.get(jats_url, timeout=30)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            console.print(f"[yellow]Failed to fetch JATS for DOI {row.get('doi')}: {e}[/]")
            return None

        # catch XML parse errors specifically (lxml.XMLSyntaxError or ElementTree.ParseError)
        from xml.etree.ElementTree import ParseError as ET_ParseError
        xml_parse_exc = getattr(etree, "XMLSyntaxError", ET_ParseError)
        try:
            authors = parse_authors_and_affiliations_from_jats(r.text)
        except (ET_ParseError, xml_parse_exc) as e:
            console.print(f"[yellow]Failed to parse JATS for DOI {row.get('doi')}: {e}[/]")
            return None

        # normalize affiliation compare to target
        def has_target(afflist: list[str]) -> bool:
            return any(target_affil.strip().lower() in (a or "").strip().lower() for a in afflist)

        first_two = authors[:2]
        last_two = authors[-2:]

        first_two_has = any(has_target(a["affiliations"]) for a in first_two) if first_two else False
        last_two_has = any(has_target(a["affiliations"]) for a in last_two) if last_two else False
        any_has = any(has_target(a["affiliations"]) for a in authors) if authors else False

        augmented = dict(row)
        augmented["first_two_has_target"] = first_two_has
        augmented["last_two_has_target"] = last_two_has
        augmented["any_author_has_target"] = any_has

        if any_has and (first_two_has or last_two_has):
            primary_rows.append(augmented)
        elif any_has:
            other_rows.append(augmented)

    # run in parallel
    with futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        future_map = {ex.submit(analyze_row, r.as_dict()): r for r in df.iter_rows(named=True)}
        for fut in futures.as_completed(future_map):
            exc = fut.exception()
            if exc is not None:
                console.print(f"[red]Error analyzing row: {exc}[/]")

    # write outputs
    if primary_rows:
        primary_df = pl.DataFrame(primary_rows)
        primary_out = outdir / f"primary_affil_{target_affil.replace(' ', '_')}.parquet"
        if "abstract" in primary_df.columns:
            primary_df = primary_df.drop("abstract")
        primary_df.write_parquet(primary_out)
        console.print(f"Wrote {len(primary_df)} rows to {primary_out}")

    if other_rows:
        other_df = pl.DataFrame(other_rows)
        other_out = outdir / f"other_affil_{target_affil.replace(' ', '_')}.parquet"
        if "abstract" in other_df.columns:
            other_df = other_df.drop("abstract")
        other_df.write_parquet(other_out)
        console.print(f"Wrote {len(other_df)} rows to {other_out}")


# parse_jats is an importable module; CLI entrypoints are provided by biorxiv_search.py
