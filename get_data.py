"""Download a dataset into ./data. Zip archives are extracted automatically.

Usage:
    uv run python get_data.py                  # downloads the default UCI dataset
    uv run python get_data.py --url <URL>       # downloads from a custom URL

Default source:
https://archive.ics.uci.edu/dataset/1276/amazon+product+and+google+locations+reviews
"""

import argparse
import io
import re
import urllib.request
import zipfile
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_URL = (
    "https://archive.ics.uci.edu/static/public/1276/"
    "amazon+product+and+google+locations+reviews.zip"
)
DATA_DIR = Path(__file__).parent / "data"

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
    print(f"Downloading from {url} ...")
    with urllib.request.urlopen(url) as response:
        return response.read()


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="URL to download data from (zip archives are extracted automatically)",
    )
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    url = resolve_url(args.url)
    content = download(url)
    saved = save(content, url)

    print("Done. Saved files:")
    for path in saved:
        print(f"  {path}")


if __name__ == "__main__":
    main()
