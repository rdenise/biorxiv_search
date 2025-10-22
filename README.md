# Biorxiv_search

Search biorXiv / medRxiv for records whose corresponding author affiliation matches an exact string (case-insensitive).

## Features

- Query by server: `biorxiv`, `medrxiv`, or `both`.
- Date range filtering (YYYY-MM-DD).
- Exact (trimmed, case-insensitive) matching on `author_corresponding_institution`.
- Outputs per-server CSVs and an optional combined CSV when querying both servers.

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

### CLI

Run the CLI from the repository root:

```bash
python biorxiv_search.py search --server biorxiv --start 2013-01-01 --end 2025-10-22 --target "Institut pasteur" --outdir ./out
```

Options:

- `--server`: `biorxiv`, `medrxiv`, or `both` (default: `both`).
- `--start`: start date (YYYY-MM-DD), default `2013-01-01`.
- `--end`: end date (YYYY-MM-DD), default = today.
- `--target`: target affiliation to match exactly (case-insensitive), default `Institut pasteur`.
- `--pause`: pause between paginated requests in seconds (default 0.2).
- `--outdir`: directory to write CSV output files (default: current working directory).

Output files are named `<server>_<target>_exact.csv` (spaces in the target are replaced with underscores). When `--server both` is used, an additional combined CSV is written and namesd `both_<target>_exact.csv`.

## Error handling

- The fetch logic includes retries with exponential backoff for transient network errors and HTTP 421 responses. If retries are exhausted, the function raises a `requests.exceptions.RequestException`.
- The CLI prints rich-styled progress and error messages.

## Notes

- The project uses the public `https://api.biorxiv.org/` endpoint. Be respectful with request frequency when querying large date ranges.
- If you plan to run this repeatedly or at scale, consider caching results or using the API’s bulk options if available.

## License

This project is licensed under the MIT License. See the LICENSE file for the full text.

SPDX-License-Identifier: MIT
