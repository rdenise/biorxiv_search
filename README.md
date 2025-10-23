# Biorxiv_search

Search biorXiv / medRxiv for records whose corresponding author affiliation matches an exact string (case-insensitive).

## Features

- Query by server: `biorxiv`, `medrxiv`, or `both`.
- Date range filtering (YYYY-MM-DD).
- Exact (trimmed, case-insensitive) matching on `author_corresponding_institution`.
- **Parallel page fetching** with configurable concurrency for faster searches.
- **Resume capability** via temporary batch files - interrupted searches can continue from where they stopped.
- **Progress tracking** with real-time spinner showing pages fetched and estimated article count.
- **Configuration summary** displayed at start showing all parameters for the run.
- **Robust error handling** with automatic retries for transient failures (HTTP 421, 500, network errors).
- **Smart resume detection** - displays already completed work when resuming interrupted searches.
- Outputs per-server CSVs and an optional combined CSV when querying both servers.
- Date range information encoded in output filenames for easy tracking.

## Requirements

- Python 3.10+ (tested on 3.11/3.14 environments)
- Dependencies:
  - requests
  - polars
  - rich
  - rich-click

You can install the dependencies with pip:

```bash
python -m pip install requests polars rich rich-click
```

You can install the dependencies with conda/mamba:

```bash
mamba create -n biorxiv -c conda-forge python=3.14 requests polars rich-click
```

the name of the environment here was set to `biorxiv` and to activate the environment:

```bash
mamba activate biorxiv
```

(or from a virtualenv / conda environment of your choice)

## Usage

### CLI Commands

The tool provides two main commands:

1. **`search`** - Search for records matching a specific affiliation and output CSV files
2. **`fetch`** - Fetch all records (no affiliation filtering) and output Parquet files

### Command: `search`

Search for records with matching corresponding author affiliation and write CSV output.

**Basic usage:**

```bash
python biorxiv_search.py search --target "Institut Pasteur"
```

**Full example with all options:**

```bash
python biorxiv_search.py search \
  --server both \
  --start 2020-01-01 \
  --end 2024-12-31 \
  --target "Institut Pasteur" \
  --pause 0.2 \
  --cpu 8 \
  --max-retries 50 \
  --temp-outdir ./resume_data \
  --outdir ./results
```

**Options:**

- `--server`: `biorxiv`, `medrxiv`, or `both` (default: `both`).
- `--start`: start date (YYYY-MM-DD), default `2013-01-01`.
- `--end`: end date (YYYY-MM-DD), default = today.
- `--target`: target affiliation to match exactly (case-insensitive), default `Institut Pasteur`.
- `--pause`: pause between batches when polling the executor (seconds), default `0.2`.
- `--cpu`: number of concurrent page requests (parallel fetching), default `4`.
- `--max-retries`: maximum retry attempts per page for transient failures, default `50`.
- `--temp-outdir`: directory for temporary batch files (enables resume capability). If not specified, a temporary directory is created and removed on completion.
- `--outdir`: directory to write CSV output files (default: current working directory).

**Output files:**

Files are named: `{server}_{target}_{start_date}_{end_date}_extract.csv`

Examples:
- `biorxiv_Institut_Pasteur_20200101_20241231_extract.csv`
- `medrxiv_Institut_Pasteur_20200101_20241231_extract.csv`
- `both_Institut_Pasteur_20200101_20241231_extract.csv` (combined, when `--server both`)

Dates in filenames are formatted without dashes (YYYYMMDD).

**Resume capability:**

If you specify `--temp-outdir`, the search creates per-page parquet files that allow resuming interrupted searches:

```bash
# First run (gets interrupted)
python biorxiv_search.py search \
  --temp-outdir ./resume_data \
  --target "Institut Pasteur"

# Resume from where it stopped (same command)
python biorxiv_search.py search \
  --temp-outdir ./resume_data \
  --target "Institut Pasteur"
```

The tool automatically detects existing batch files and continues from the last completed page. When resuming, you'll see:
- A summary of already completed pages and articles
- Progress tracking that includes previously completed work
- Automatic detection and filling of any missing pages

### Command: `fetch`

Fetch all records (no affiliation filtering) and write Parquet output files.

**Basic usage:**

```bash
python biorxiv_search.py fetch --server both
```

**Full example:**

```bash
python biorxiv_search.py fetch \
  --server both \
  --start 2024-01-01 \
  --end 2024-12-31 \
  --concurrency 4 \
  --outdir ./parquet_output
```

**Options:**

- `--server`: `biorxiv`, `medrxiv`, or `both` (default: `both`).
- `--start`: start date (YYYY-MM-DD), default `2013-01-01`.
- `--end`: end date (YYYY-MM-DD), default = today.
- `--concurrency`: number of concurrent page requests, default `2`.
- `--outdir`: directory to write Parquet output files (default: `out_parquet`).

**Output files:**

- `biorxiv.parquet` - all biorxiv records
- `medrxiv.parquet` - all medrxiv records  
- `combined.parquet` - combined records (when `--server both`)

The `fetch` command also supports resume via temporary batch files stored in `{outdir}/temp_batches/`.

## Error handling

- **Automatic retries**: The fetch logic includes automatic retries with exponential backoff for:
  - Transient network errors (timeouts, connection resets)
  - HTTP 421 (Misdirected Request)
  - HTTP 500 (Internal Server Error)
- **Configurable retry limit**: Use `--max-retries` to control the maximum number of retry attempts per page (default: 50).
- **Resume from failures**: If a search is interrupted, use `--temp-outdir` to resume from the last successfully fetched page.
- **Missing pages detection**: The tool automatically detects and fetches any missing pages when resuming.
- **Rich progress output**: The CLI prints colored progress and error messages for easy monitoring.

If retries are exhausted for a specific page, the function raises a `requests.exceptions.RequestException`.

## Module Structure

The project consists of several Python modules:

- **`biorxiv_search.py`** - Main CLI interface using rich-click
- **`search.py`** - Core search functions with affiliation filtering
  - `fetch_exact_affil()` - Searches for records matching a target affiliation
  - Supports parallel page fetching, resume capability, and retry logic
- **`fetch.py`** - Parallel fetching utilities for bulk data downloads
  - `fetch_servers()` - Fetches from multiple servers
  - `fetch_all_records()` - Fetches all records (no filtering) with parallel page fetching
  - `write_parquet_tables()` - Writes output as Parquet files
- **`requirements.txt`** - Python package dependencies

## Visual Progress Feedback

Both `search` and `fetch` commands provide rich visual feedback during execution:

- **Configuration Summary**: At the start, a boxed table shows all run parameters
- **Progress Indicator**: A spinner animation shows the tool is working
- **Page Counter**: Displays the number of pages fetched
- **Article Estimate**: Shows approximate article count (~pages × 100)
- **Resume Information**: When resuming, displays already completed pages and articles

Example progress output:
```
⠋ Fetching biorxiv... 45 ~4,500 articles done
```

When resuming:
```
[yellow]Resuming fetch for biorxiv from cursor 4600 (after max existing 4500).[/]
[green]Already completed: 45 pages (~4,500 articles)[/]
⠋ Fetching biorxiv... 46 ~4,600 articles done
```

## Performance Tips

1. **Parallel fetching**: Use `--cpu 8` or higher for faster searches (I/O bound workload)
2. **Resume capability**: Use `--temp-outdir` for long-running searches to enable resume
3. **Monitor progress**: The real-time progress counter helps estimate remaining time
4. **Be respectful**: The API is public - avoid excessive concurrency or very short pause times
5. **Large date ranges**: Consider breaking into smaller chunks or using the `fetch` command for bulk downloads

## Notes

- The project uses the public `https://api.biorxiv.org/` endpoint. Be respectful with request frequency when querying large date ranges.
- If you plan to run this repeatedly or at scale, consider caching results or using the API's bulk options if available.
- The tool automatically deduplicates records by DOI, keeping the highest version number.
- The `abstract` field is dropped from all outputs to reduce file size.

## License

This project is licensed under the MIT License. See the LICENSE file for the full text.

SPDX-License-Identifier: MIT
