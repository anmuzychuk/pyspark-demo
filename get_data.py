"""Download a dataset into ./data. Zip archives are extracted automatically.

Usage:
    uv run python get_data.py                        # the default dataset (reviews)
    uv run python get_data.py --dataset taxi         # a named dataset (see DATASETS)
    uv run python get_data.py --url <URL>            # any other URL
    uv run python get_data.py --list                 # what is already downloaded

GitHub "blob" page URLs are rewritten to their raw-content equivalent, so you can paste
the link straight out of the browser.
"""

import argparse
import io
import re
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from urllib.parse import urlparse

DATA_DIR = Path(__file__).parent / "data"

LEARNING_SPARK = (
    "https://github.com/databricks/LearningSparkV2/blob/master/databricks-datasets/learning-spark-v2"
)

# Named datasets: url -> the file it produces in ./data. The second entry is what lets us
# skip a download we have already done, which a zip archive could not tell us on its own.
DATASETS = {
    # Amazon product / Google locations reviews (UCI), shipped as a zip.
    "reviews": (
        "https://archive.ics.uci.edu/static/public/1276/"
        "amazon+product+and+google+locations+reviews.zip",
        "y_amazon-google-large.csv",
    ),
    "mnm": (f"{LEARNING_SPARK}/mnm_dataset.csv", "mnm_dataset.csv"),
    "flights": (f"{LEARNING_SPARK}/flights/departuredelays.csv", "departuredelays.csv"),
    # NYC Taxi & Limousine Commission trip records, January 2023 (~46 MB). Used by labs/L2.
    "taxi": (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "yellow_tripdata_2023-01.parquet",
    ),
}
DEFAULT_DATASET = "reviews"

GITHUB_BLOB_RE = re.compile(
    r"^https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/blob/(?P<ref>[^/]+)/(?P<path>.+)$"
)


def resolve_url(url: str) -> str:
    """Rewrite a GitHub 'blob' page URL to its raw-content equivalent."""
    match = GITHUB_BLOB_RE.match(url)
    if match:
        return (
            f"https://raw.githubusercontent.com/{match['owner']}/{match['repo']}"
            f"/{match['ref']}/{match['path']}"
        )
    return url


def download(url: str) -> bytes:
    """Fetch a URL, printing progress as it goes. Some of these files are tens of MB."""
    print(f"Downloading {url} ...")
    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            total = int(response.headers.get("Content-Length", 0))
            chunks = []
            written = 0
            while chunk := response.read(1 << 20):
                chunks.append(chunk)
                written += len(chunk)
                if total:
                    print(f"\r  {100 * written / total:5.1f}%  {written / 1024**2:6.0f} MB", end="")
            if total:
                print()
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Could not reach {url}: {exc.reason}") from exc
    return b"".join(chunks)


def save(content: bytes, url: str) -> list[Path]:
    if zipfile.is_zipfile(io.BytesIO(content)):
        print(f"Extracting zip archive to {DATA_DIR} ...")
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            zf.extractall(DATA_DIR)
            return [DATA_DIR / name for name in zf.namelist()]

    filename = Path(urlparse(url).path).name or "downloaded_file"
    dest = DATA_DIR / filename
    dest.write_bytes(content)
    return [dest]


def destination_for(url: str) -> Path:
    """Where a non-zip download would land. Used to skip work we have already done."""
    return DATA_DIR / (Path(urlparse(url).path).name or "downloaded_file")


def list_downloaded() -> int:
    files = sorted(p for p in DATA_DIR.rglob("*") if p.is_file()) if DATA_DIR.exists() else []
    if not files:
        print(f"Nothing in {DATA_DIR}")
        return 0
    print(f"In {DATA_DIR}:")
    for f in files:
        print(f"  {f.relative_to(DATA_DIR).as_posix():<50} {f.stat().st_size / 1024**2:6.1f} MB")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASETS),
        help=f"named dataset to download (default: {DEFAULT_DATASET})",
    )
    parser.add_argument("--url", help="download from a custom URL instead of a named dataset")
    parser.add_argument("--force", action="store_true", help="re-download even if the file is already there")
    parser.add_argument("--list", action="store_true", help="list what is already in ./data and exit")
    args = parser.parse_args()

    if args.list:
        return list_downloaded()

    if args.url and args.dataset:
        parser.error("pass --dataset or --url, not both")

    if args.url:
        url, dest = resolve_url(args.url), None
    else:
        url, expected = DATASETS[args.dataset or DEFAULT_DATASET]
        url, dest = resolve_url(url), DATA_DIR / expected

    DATA_DIR.mkdir(exist_ok=True)
    if dest is None:
        dest = destination_for(url)
    if dest.exists() and not args.force:
        print(f"Already have {dest} ({dest.stat().st_size / 1024**2:.1f} MB). Use --force to re-download.")
        return 0

    saved = save(download(url), url)

    print("Done. Saved files:")
    for path in saved:
        print(f"  {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
