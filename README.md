# PySpark on a local machine

A small playground for learning Apache Spark with PySpark on your own laptop — no cluster,
no cloud account. Everything runs in local mode against a JVM on your machine.

## Setup

Follow the guide for your platform, then come back here:

- **macOS** — [`getting_started/local_setup_mac.md`](getting_started/local_setup_mac.md)
- **Windows** — [`getting_started/local_setup_windows.md`](getting_started/local_setup_windows.md)

Both cover installing a JDK and [uv](https://docs.astral.sh/uv/), syncing the project,
running an interactive PySpark shell, submitting a job with `spark-submit`, and using Spark
from a Jupyter notebook. Each ends with a troubleshooting section for the errors you are
most likely to hit.

Short version, once the prerequisites are in place:

```bash
uv sync
uv run python main.py
```

## What is in here

| Path | What it is |
|---|---|
| `main.py` | Smallest possible session — run it to confirm your setup works |
| `get_data.py` | Downloads the datasets everything else uses, into `data/` |
| `mnmcount.py` | A standalone job, meant to be run with `spark-submit` |
| `getting_started.ipynb` | Notebook tour: DataFrames, the Spark UI, RDDs, managed tables |
| `labs/` | Graded lab assignments |
| `getting_started/` | Platform setup guides |

## Datasets

Nothing is committed; fetch what you need into `data/` (git-ignored):

```bash
uv run python get_data.py --dataset reviews   # Amazon/Google reviews (default)
uv run python get_data.py --dataset mnm       # M&M counts, used by mnmcount.py
uv run python get_data.py --dataset flights   # US departure delays
uv run python get_data.py --dataset taxi      # NYC yellow taxi, Jan 2023 (~46 MB), used by labs/L2
uv run python get_data.py --list              # what you already have
```

Any other URL works too — GitHub `blob` links are resolved to raw content, and zip archives
are extracted automatically:

```bash
uv run python get_data.py --url <URL>
```

## Running things

```bash
uv run python main.py                                    # plain script
uv run pyspark                                           # interactive shell
uv run spark-submit mnmcount.py data/mnm_dataset.csv     # submit a job
uv run jupyter lab                                       # notebooks, including the labs
```

While a session is running, the Spark UI is at <http://localhost:4040>.

## Labs

Graded assignments live under `labs/`, one notebook per lab, each with its own `README.md`
carrying the objectives, tasks and rubric. Open them through `uv run jupyter lab` or in
VS Code; they find the repo root themselves, so it does not matter where you launch from.

## Maintaining this repo

Running a notebook fills it with outputs, and committing those bloats the history and makes
every diff unreadable. Two one-time commands stop that happening by accident:

```bash
uv run nbstripout --install          # strip outputs from notebooks as they are committed
git config core.hooksPath .githooks  # refuse a push that carries outputs anyway
```

The first registers a git *clean filter*, declared for `*.ipynb` in `.gitattributes`. Your
working copy keeps its outputs — you can still read your results after a run — while what
git stores is stripped. The second enables `.githooks/pre-push`, which catches the case where
the filter was never installed: git silently ignores a filter it does not have configured, so
without the hook that failure is invisible.

Neither is installed automatically when you clone; git deliberately does not run hooks or
filters from a repository you just downloaded.

**Students: skip both.** If you forked this repo to work through the labs, you *want* your
outputs committed. The repo behaves normally without either command.
