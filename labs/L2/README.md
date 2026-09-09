# Lab 2 — Spark execution: partitions, shuffles and the UI

**Time:** ~90 minutes
**Points:** 4 (personal assignment)
**Due:** one week after the lecture

## Objectives

1. Read a Spark execution plan and tell narrow from wide transformations in it
2. Find a job in the Spark UI and explain where its time went
3. Predict, then measure, the effect of partition count on a job
4. Observe AQE turning a bad plan into a good one
5. Recognise skew from the task duration distribution

## Prerequisites

A working local Spark setup — [`getting_started/local_setup_mac.md`](../../getting_started/local_setup_mac.md)
or [`getting_started/local_setup_windows.md`](../../getting_started/local_setup_windows.md).

Then, from the **repo root**:

```sh
uv sync
uv run python get_data.py --dataset taxi
```

That fetches one month of NYC yellow taxi trips (~46 MB) into `data/`.

## The lab notebook

**`lab.ipynb`** — run it in JupyterLab:

```sh
uv run jupyter lab          # then open labs/L2/lab.ipynb
```

or open it in VS Code and select `.venv/bin/python` as the kernel.

You can launch Jupyter from anywhere in the repo. The setup cell walks up to the repo root
and makes it the working directory, so the dataset path resolves and Spark keeps its
`spark-warehouse/` and `metastore_db/` there rather than scattering copies under `labs/`.

Run the cells in order. Exercises 3 and 5 send you to the Spark UI at
<http://localhost:4040>, which only exists while the kernel's session is alive — so leave
the last cell, `spark.stop()`, until you have captured both screenshots.

> If your laptop is slow, trim the last one or two values from `SWEEP` in Exercise 1. The
> shape of the curve is what matters, not the absolute numbers.

## Tasks

`lab.ipynb` has five exercises:

| # | Topic | What you produce |
|---|---|---|
| 1 | Partitions and parallelism | A table of runtime vs partition count, and an explanation of the shape |
| 2 | Narrow vs wide | Classify six operations, then verify each against the physical plan |
| 3 | Reading the Spark UI | Which stage dominated, and why |
| 4 | AQE | Same query with AQE off and on; explain the plan difference |
| 5 | Skew | Build a skewed join, observe it, fix it |

Every exercise has a written component. The writing is what is being marked — the code
is mostly given.

The notebook ends with an **appendix on the catalog / database / table hierarchy** — where a
table's metadata and its bytes actually live, and what changes when you point Spark at a
different catalog. It is not marked and does not affect the rubric, but it explains the
`metastore_db/` and `spark-warehouse/` directories that appear in your repo root, and it is
the local counterpart to Unity Catalog.

## Deliverable

`L2_<your-surname>.ipynb` — a copy of `lab.ipynb` with:

- all `# TODO` cells completed
- all written answers filled in (the markdown cells that say *replace this line*)
- **two screenshots** of the Spark UI pasted into the notebook (exercises 3 and 5).
  Use a markdown cell with an embedded image, or attach them separately with clear names.
- outputs left in place, so the marker can see your measured numbers without re-running it.

## Rubric (4 points)

| | Points |
|---|---|
| Ex 1 — measurements taken, curve explained | 0.5 |
| Ex 2 — all six classified correctly, verified against the plan | 0.5 |
| Ex 3 — correct stage identified, explanation is about *shuffle*, not "it was big" | 1 |
| Ex 4 — plan difference described in terms of what AQE actually changed | 1 |
| Ex 5 — skew observed in task durations and a working mitigation applied | 1 |

Answers that restate the slides get half marks. Answers that cite your own measured
numbers get full marks.

## Hints

- The Spark UI URL is printed by the session cell — normally <http://localhost:4040>.
- `df.explain()` prints the physical plan — the one that actually ran. Look for `Exchange`:
  that node **is** a shuffle. `df.explain(True)` additionally shows the parsed, analysed and
  optimised plans. Exercise 2 uses the `physical_plan()` helper, which is just `explain()`
  with its output captured into a string so you can search it.
- Exercise 4 rebuilds the query inside a function on purpose. A DataFrame caches its
  QueryExecution the first time it is planned, so reusing one object across a
  `spark.conf.set` hands you the *old* plan back and the comparison silently shows nothing.
- The session is `local[4]`, not `local[*]`, because the lab measures parallelism and the
  slot count has to be a number you know.
