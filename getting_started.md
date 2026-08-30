# Getting Started: PySpark on macOS

This guide walks you through setting up this repo on a Mac (Intel or Apple
Silicon) using [uv](https://docs.astral.sh/uv/) as the package manager.

## Prerequisites

PySpark runs on the JVM under the hood, so you need two things installed:

1. **Java** — a JDK (PySpark 4.x requires Java 17 or 21)
2. **uv** — manages the Python version and dependencies for this project

### 1. Install Homebrew (if you don't have it)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Install Java

```bash
brew install openjdk@17
```

`openjdk@17` is keg-only (not symlinked into `/opt/homebrew` by default), so
PySpark needs to be told where to find it. Add this to your `~/.zshrc`:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"
```

Then reload your shell config and verify:

```bash
source ~/.zshrc
java -version
```

You should see output like `openjdk version "17.0.x"`.

> **Apple Silicon vs Intel:** Homebrew installs to `/opt/homebrew` on Apple
> Silicon Macs and `/usr/local` on Intel Macs. If you're on an Intel Mac,
> use `export JAVA_HOME="/usr/local/opt/openjdk@17"` instead.

### 3. Install uv

```bash
brew install uv
```

Verify:

```bash
uv --version
```

## Project Setup

### 1. Clone the repo and move into it

```bash
git clone <repo-url>
cd pyspark-demo
```

### 2. Sync dependencies

`uv` reads `pyproject.toml`, downloads the right Python version (see
`.python-version`) automatically, and creates a `.venv` for you:

```bash
uv sync
```

### 3. Run the demo

```bash
uv run python main.py
```

If everything is set up correctly, you should see a small PySpark
DataFrame printed to the console along with an average age calculation.

### 4. Download the sample dataset

`get_data.py` downloads a dataset into `./data`, extracting it automatically
if it's a zip archive:

```bash
uv run python get_data.py
```

To use a different dataset, pass `--url`. GitHub `blob` page URLs are
resolved to their raw content automatically:

```bash
uv run python get_data.py --url https://github.com/databricks/LearningSparkV2/blob/master/databricks-datasets/learning-spark-v2/mnm_dataset.csv
```

## Troubleshooting

**`Unable to locate a Java Runtime` / `JAVA_GATEWAY_EXITED`**
Java isn't installed or `JAVA_HOME` isn't set correctly. Re-check step 2
above and make sure `java -version` works in a fresh terminal.

**Multiple Java versions installed / wrong version picked up**
Explicitly check `echo $JAVA_HOME` and confirm it points at the `openjdk@17`
install. You can list installed JDKs with `brew list --formula | grep openjdk`.

**Works in a terminal but fails in Jupyter with `Unable to locate a Java Runtime`**
Notebook kernels launched from an IDE (VS Code, PyCharm) or a GUI app (Jupyter
Desktop) don't source `~/.zshrc`, so `JAVA_HOME` may never reach the kernel
process even if it's set in your shell profile. `getting_started.ipynb`
handles this itself — its first cell resolves `JAVA_HOME` via
`brew --prefix openjdk@17` at runtime instead of relying on the shell
environment. If you write your own notebook, copy that pattern into your
first cell before creating a `SparkSession`.

**`WARN Utils: Your hostname ... resolves to a loopback address`**
This is a harmless warning on machines where the hostname doesn't resolve
cleanly to a LAN IP — Spark still runs fine locally. You can ignore it.

**`$SPARK_HOME` is empty**
This is expected — `uv add pyspark` installs PySpark as a Python package,
not a standalone Spark distribution, so nothing sets `SPARK_HOME`
automatically. See [Submitting a Spark Job with
spark-submit](#submitting-a-spark-job-with-spark-submit) below.

## Using PySpark Interactively

For exploration, you can start a PySpark shell directly:

```bash
uv run pyspark
```

Or launch a Python REPL with the venv active:

```bash
uv run python
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.master("local[*]").getOrCreate()
```

## Submitting a Spark Job with spark-submit

Some Spark tutorials (e.g. *Learning Spark*) have you run scripts via
`$SPARK_HOME/bin/spark-submit`. With a `uv`-managed install there's no
separate Spark distribution to point `SPARK_HOME` at — `uv sync` installs
`spark-submit` straight into `.venv/bin` alongside `pyspark`, `pyspark-shell`,
etc. So the simplest way to run a job is:

```bash
uv run spark-submit mnmcount.py data/mnm_dataset.csv
```

`uv run` puts `.venv/bin` first on `PATH`, which also fixes a subtler issue:
`spark-submit` shells out to whatever `python3` it finds on `PATH`, and on
macOS that's usually the old Xcode Command Line Tools stub (Python 3.9),
which is too old for PySpark 4.x and fails with
`TypeError: unsupported operand type(s) for |: 'type' and 'type'`.
`uv run` ensures it picks up the project's Python 3.13 venv instead.

If you specifically need `$SPARK_HOME` set (e.g. following a book/tutorial
literally, or submitting to a remote cluster later), point it at the
`pyspark` package inside the venv and also set `PYSPARK_PYTHON` so
`spark-submit` doesn't fall back to the system Python:

```bash
export SPARK_HOME="$(uv run python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
"$SPARK_HOME/bin/spark-submit" mnmcount.py data/mnm_dataset.csv
```

## Adding New Dependencies

Use `uv add` instead of `pip install` so `pyproject.toml` and the lockfile
stay in sync:

```bash
uv add pandas
```
