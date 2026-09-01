# Getting Started: PySpark on Windows

This guide walks you through setting up this repo on Windows 10/11 using
[uv](https://docs.astral.sh/uv/) as the package manager. It mirrors
`getting_started.md` (macOS), but Windows needs a few extra steps that Unix
systems don't — mainly Hadoop's `winutils.exe` and telling Spark which
Python interpreter to use.

All commands below assume **PowerShell** (the default terminal in Windows
Terminal and VS Code). Where Command Prompt (`cmd.exe`) differs, it's noted.

## Prerequisites

PySpark runs on the JVM under the hood, so you need:

1. **Java** — a JDK (PySpark 4.x requires Java 17 or 21)
2. **uv** — manages the Python version and dependencies for this project
3. **winutils.exe / hadoop.dll** — Hadoop's Windows native shims, needed as
   soon as Spark writes files (Parquet output, `spark-warehouse`, checkpoints)

### 1. Install Java

The easiest route is `winget`, which ships with Windows 11 and recent
Windows 10 builds:

```powershell
winget install --id EclipseAdoptium.Temurin.17.JDK
```

(If you prefer a manual install, grab the Temurin 17 **MSI** from
<https://adoptium.net/> and tick "Set JAVA_HOME variable" in the installer.)

Then set `JAVA_HOME` **for your user account** so every new terminal, IDE and
Jupyter kernel picks it up:

```powershell
[Environment]::SetEnvironmentVariable(
    "JAVA_HOME",
    "C:\Program Files\Eclipse Adoptium\jdk-17.0.13.11-hotspot",
    "User"
)
```

Adjust the version folder to what actually got installed — list it with:

```powershell
Get-ChildItem "C:\Program Files\Eclipse Adoptium"
```

Add Java to `PATH` as well. Note the expanded path rather than
`%JAVA_HOME%\bin` — `SetEnvironmentVariable` stores a plain string, so a
`%VAR%` reference written this way is never expanded:

```powershell
$userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
$javaBin  = Join-Path ([Environment]::GetEnvironmentVariable("JAVA_HOME", "User")) "bin"
[Environment]::SetEnvironmentVariable("PATH", "$userPath;$javaBin", "User")
```

(If you used the Temurin MSI and ticked "Add to PATH", this is already done —
check with `echo $env:PATH` in a new terminal before appending a duplicate.)

> Environment variables set this way only apply to **newly opened** terminals.
> Close and reopen PowerShell (and restart VS Code entirely) before verifying.

Verify in a fresh terminal:

```powershell
java -version
echo $env:JAVA_HOME
```

You should see `openjdk version "17.0.x"`.

> **Avoid spaces if you hit trouble.** Spark's launcher scripts are generally
> fine with `C:\Program Files\...`, but if you see odd
> `'C:\Program' is not recognized` errors, reinstall the JDK to a path without
> spaces, e.g. `C:\Java\jdk-17`.

### 2. Install uv

```powershell
winget install --id astral-sh.uv
```

Or via the official installer script:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Verify (in a fresh terminal, so the updated `PATH` is picked up):

```powershell
uv --version
```

### 3. Install winutils.exe and hadoop.dll

Spark on Windows calls into Hadoop's native Windows binaries for any
filesystem operation beyond plain reads. Without them you'll hit
`java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`
or `UnsatisfiedLinkError: ... NativeIO$Windows.access0`.

Download the two files for Hadoop 3.3.x from the community-maintained
[cdarlint/winutils](https://github.com/cdarlint/winutils) repo and put them
in a `bin` subfolder:

```powershell
New-Item -ItemType Directory -Force -Path C:\hadoop\bin | Out-Null

$base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
Invoke-WebRequest "$base/winutils.exe" -OutFile C:\hadoop\bin\winutils.exe
Invoke-WebRequest "$base/hadoop.dll"   -OutFile C:\hadoop\bin\hadoop.dll
```

Point `HADOOP_HOME` at the **parent** of `bin` (not at `bin` itself) and add
`bin` to `PATH`:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[Environment]::SetEnvironmentVariable(
    "PATH",
    "$([Environment]::GetEnvironmentVariable('PATH','User'));C:\hadoop\bin",
    "User"
)
```

Reopen your terminal and verify:

```powershell
winutils.exe systeminfo
```

Any output (even a wall of numbers) means it runs. If Windows reports a
missing DLL, install the
[Visual C++ 2015–2022 Redistributable (x64)](https://aka.ms/vs/17/release/vc_redist.x64.exe).

> `hadoop.dll` must also be loadable by the JVM. Having `C:\hadoop\bin` on
> `PATH` covers that. If you still get `UnsatisfiedLinkError`, copy
> `hadoop.dll` into `C:\Windows\System32` as a fallback.

## Project Setup

### 1. Clone the repo and move into it

```powershell
git clone <repo-url>
cd pyspark-demo
```

> **Keep the path short and space-free**, e.g. `C:\dev\pyspark-demo`. Spark
> writes deeply nested temp directories, and paths under
> `C:\Users\<you>\OneDrive\Documents\...` can blow past the legacy 260-char
> limit. Cloning into a OneDrive-synced folder also causes file-locking errors
> while Spark writes — prefer a local, non-synced directory.

### 2. Sync dependencies

`uv` reads `pyproject.toml`, downloads the right Python version (see
`.python-version`, currently 3.13) automatically, and creates a `.venv` for
you:

```powershell
uv sync
```

On Windows the virtualenv layout differs from macOS/Linux: executables live
in `.venv\Scripts\` rather than `.venv/bin/`, and the interpreter is
`.venv\Scripts\python.exe`.

### 3. Tell Spark which Python to use

This is the single most common Windows-specific failure. Spark launches
Python worker processes by running whatever the `PYSPARK_PYTHON` variable
says — and if it's unset it falls back to `python3`, which on Windows either
doesn't exist or resolves to the Microsoft Store stub that opens the Store
instead of running your code.

Set both variables **for the project**, pointing at the venv interpreter:

```powershell
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
```

That lasts for the current terminal session only. To make it permanent for
your user (recommended while working through this repo), use an absolute
path:

```powershell
[Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "C:\dev\pyspark-demo\.venv\Scripts\python.exe", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "C:\dev\pyspark-demo\.venv\Scripts\python.exe", "User")
```

In `cmd.exe` the equivalents are `set PYSPARK_PYTHON=...` (session) and
`setx PYSPARK_PYTHON "..."` (persistent).

### 4. Run the demo

```powershell
uv run python main.py
```

The first run pops a **Windows Defender Firewall** dialog asking whether to
allow Java to accept connections — Spark's driver opens local ports to talk
to its executors. Allow it on **private networks**; denying it can leave the
driver hanging on `getOrCreate()`.

If everything is set up correctly, you should see a small PySpark DataFrame
printed to the console along with an average age calculation.

### 5. Download the sample dataset

`get_data.py` downloads a dataset into `.\data`, extracting it automatically
if it's a zip archive:

```powershell
uv run python get_data.py
```

To use a different dataset, pass `--url`. GitHub `blob` page URLs are
resolved to their raw content automatically:

```powershell
uv run python get_data.py --url https://github.com/databricks/LearningSparkV2/blob/master/databricks-datasets/learning-spark-v2/mnm_dataset.csv
```

## Using PySpark Interactively

For exploration, you can start a PySpark shell directly:

```powershell
uv run pyspark
```

Or launch a Python REPL with the venv active:

```powershell
uv run python
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.master("local[*]").getOrCreate()
```

If you'd rather activate the venv once instead of prefixing every command
with `uv run`:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks that with
`running scripts is disabled on this system`, allow signed local scripts for
your user (a one-time change):

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

In `cmd.exe`, use `.venv\Scripts\activate.bat` instead — no execution policy
involved.

## Submitting a Spark Job with spark-submit

Some Spark tutorials (e.g. *Learning Spark*) have you run scripts via
`%SPARK_HOME%\bin\spark-submit`. With a `uv`-managed install there's no
separate Spark distribution to point `SPARK_HOME` at — `uv sync` installs
`spark-submit.cmd` straight into `.venv\Scripts\` alongside `pyspark.cmd`,
`spark-shell.cmd`, etc. So the simplest way to run a job is:

```powershell
uv run spark-submit mnmcount.py data\mnm_dataset.csv
```

`uv run` puts `.venv\Scripts` first on `PATH`, so the right `spark-submit.cmd`
and the right `python.exe` are found. Forward slashes work in paths too, so
`data/mnm_dataset.csv` is equally fine.

If `uv run spark-submit` isn't resolved, call the wrapper explicitly:

```powershell
uv run .venv\Scripts\spark-submit.cmd mnmcount.py data\mnm_dataset.csv
```

If you specifically need `%SPARK_HOME%` set (e.g. following a book/tutorial
literally, or submitting to a remote cluster later), point it at the `pyspark`
package inside the venv:

```powershell
$env:SPARK_HOME = uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))"
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
& "$env:SPARK_HOME\bin\spark-submit.cmd" mnmcount.py data\mnm_dataset.csv
```

## Running the Notebook

Launch Jupyter through `uv` so the kernel inherits the project venv:

```powershell
uv run jupyter lab
```

Or open `getting_started.ipynb` in VS Code and select the
`.venv\Scripts\python.exe` interpreter as the kernel.

**One change is required in the notebook on Windows.** The environment-setup
cell resolves `JAVA_HOME` by shelling out to Homebrew:

```python
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = subprocess.check_output(["brew", "--prefix", "openjdk@17"], text=True).strip()
```

`brew` doesn't exist on Windows, so that call fails with
`FileNotFoundError: [WinError 2]`. Since you set `JAVA_HOME` at the user level
in step 1, the `if` guard normally skips it — but if your kernel was started
before you set the variable, or you'd like the notebook to be portable,
replace that cell with a cross-platform version:

```python
import os
import subprocess
import sys

if "JAVA_HOME" not in os.environ:
    if sys.platform == "darwin":
        os.environ["JAVA_HOME"] = subprocess.check_output(["brew", "--prefix", "openjdk@17"], text=True).strip()
    else:
        raise RuntimeError("Set JAVA_HOME for your user account, then restart the kernel.")

os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
```

Setting `PYSPARK_PYTHON` to `sys.executable` makes Spark's Python workers use
the exact interpreter running the kernel, which sidesteps the `python3`
lookup problem entirely.

## Troubleshooting

**`Python worker exited unexpectedly` / `Cannot run program "python3"`**
`PYSPARK_PYTHON` isn't set, or points at the wrong interpreter. Windows has
no `python3.exe` except the Microsoft Store stub. Re-check
[step 3](#3-tell-spark-which-python-to-use) and confirm with:
```powershell
echo $env:PYSPARK_PYTHON
& $env:PYSPARK_PYTHON --version
```
A quick way to disable the Store stub: **Settings → Apps → Advanced app
settings → App execution aliases**, turn off `python.exe` and `python3.exe`.

**`HADOOP_HOME and hadoop.home.dir are unset`**
`winutils.exe` is missing. See
[step 3 of the prerequisites](#3-install-winutilsexe-and-hadoopdll). Note the
variable must point at `C:\hadoop`, **not** `C:\hadoop\bin`.

**`UnsatisfiedLinkError: NativeIO$Windows.access0`**
`winutils.exe` is present but `hadoop.dll` isn't loadable. Confirm
`C:\hadoop\bin` is on `PATH`, install the VC++ redistributable, and if
needed copy `hadoop.dll` into `C:\Windows\System32`.

**`Unable to locate a Java Runtime` / `JAVA_GATEWAY_EXITED`**
Java isn't installed or `JAVA_HOME` isn't set correctly. Re-check
[step 1](#1-install-java) and make sure `java -version` works in a **fresh**
terminal — variables set with `SetEnvironmentVariable`/`setx` don't affect
already-open windows.

**Works in a terminal but fails in Jupyter or VS Code**
The kernel was launched before you set the user environment variables. Fully
quit and restart VS Code (not just the kernel) — it caches the environment it
was started with.

**`WARN Utils: Your hostname ... resolves to a loopback address`**
Harmless on machines whose hostname doesn't resolve to a LAN IP — Spark still
runs fine locally. To silence it, set `$env:SPARK_LOCAL_IP = "127.0.0.1"`.

**`WARN Shell: Did not find winutils.exe` but jobs still work**
Read-only operations don't need the native shims; you'll only hit a hard
failure once Spark writes output. Install winutils anyway — `mnmcount.py`
and the notebook do write.

**Job hangs at `getOrCreate()` with no output**
The firewall prompt was dismissed or denied. Allow `java.exe` through
Windows Defender Firewall on private networks (**Settings → Network &
Internet → Windows Firewall → Allow an app through firewall**).

**`The filename or extension is too long` / `path too long`**
Move the repo to a short path like `C:\dev\pyspark-demo`, and optionally
enable long paths:
```powershell
# run in an elevated (Administrator) PowerShell
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
  -Name LongPathsEnabled -Value 1 -PropertyType DWORD -Force
```

**`spark-warehouse` or temp files locked / `AccessDeniedException` on delete**
Antivirus or OneDrive sync is holding the files Spark is trying to clean up.
Keep the repo outside synced folders and exclude the project directory from
real-time antivirus scanning.

**`$SPARK_HOME` / `%SPARK_HOME%` is empty**
This is expected — `uv add pyspark` installs PySpark as a Python package, not
a standalone Spark distribution, so nothing sets `SPARK_HOME` automatically.
See [Submitting a Spark Job with
spark-submit](#submitting-a-spark-job-with-spark-submit) above.

## Adding New Dependencies

Use `uv add` instead of `pip install` so `pyproject.toml` and the lockfile
stay in sync:

```powershell
uv add pandas
```
