# Architecture

## System context

The project has two main paths: an Airflow-managed ETL pipeline and a browser-based visualization that consumes the pipeline's final JSON output.

```mermaid
flowchart LR
    subgraph Source["External source"]
        Catalog["MIT course catalog<br/>HTML pages"]
    end

    subgraph Compose["Docker Compose: local Airflow stack"]
        Web["Airflow webserver"]
        Scheduler["Airflow scheduler"]
        Worker["Celery worker"]
        Metadata[("PostgreSQL 13<br/>Airflow metadata")]
        Broker[("Redis<br/>Celery broker")]

        Web --> Metadata
        Scheduler --> Metadata
        Scheduler --> Broker
        Broker --> Worker
        Worker --> Metadata
    end

    subgraph Pipeline["assignment DAG: sequential tasks"]
        T0["task_zero<br/>Install BeautifulSoup4"]
        T1["task_one: catalog()<br/>Download HTML"]
        T2["task_two: combine()<br/>Write combo.txt"]
        T3["task_three: titles()<br/>Extract h3 text"]
        T4["task_four: clean()<br/>Remove punctuation, digits,<br/>one-character words, stop words;<br/>normalize case"]
        T5["task_five: count_words()<br/>Count with Counter"]

        T0 --> T1 --> T2 --> T3 --> T4 --> T5
    end

    subgraph Artifacts["Pipeline data artifacts"]
        Raw["HTML files"]
        Combined["combo.txt"]
        Titles["titles.json"]
        Clean["titles_clean.json"]
        Counts["words.json"]
        Raw --> Combined --> Titles --> Clean --> Counts
    end

    Catalog --> T1
    Worker -. executes .-> T0
    Worker -. executes .-> T1
    Worker -. executes .-> T2
    Worker -. executes .-> T3
    Worker -. executes .-> T4
    Worker -. executes .-> T5
    T1 -. writes .-> Raw
    T2 -. writes .-> Combined
    T3 -. writes .-> Titles
    T4 -. writes .-> Clean
    T5 -. writes .-> Counts

    subgraph Visualization["Browser visualization"]
        D3["D3.js v7<br/>Packed bubble chart"]
        Legacy["D3.js v3 + d3-tip<br/>Force-directed chart"]
    end

    Counts --> D3
    Counts --> Legacy
```

## Component responsibilities

| Layer | Component | Responsibility |
|---|---|---|
| Source | MIT course catalog | Serves HTML pages containing course titles |
| Orchestration | Airflow DAG `assignment` | Declares the task order and daily schedule |
| Task execution | Celery worker | Runs the Python and Bash task callables |
| Broker | Redis | Delivers Celery work to the worker |
| Metadata | PostgreSQL | Stores Airflow DAG/task/run metadata |
| Ingestion | `urllib.request` | Retrieves catalog page responses |
| Parsing | BeautifulSoup4 | Finds `<h3>` elements and extracts their text |
| Transformation | Python string operations | Removes selected punctuation, digits, one-character tokens, and common English stop words; lowercases remaining tokens |
| Aggregation | `collections.Counter` | Produces a word-to-count mapping |
| Visualization | D3.js | Renders word frequency as packed bubbles or force-directed circles |

## Task contracts

| Order | Task callable | Input | Output |
|---|---|---|---|
| 0 | Bash package setup | — | BeautifulSoup4 installed in the worker environment |
| 1 | `catalog()` | Configured catalog URLs | One HTML file per successful URL |
| 2 | `combine()` | `*.html` files in the worker working directory | `combo.txt` |
| 3 | `titles()` | `combo.txt` | `titles.json`, an array of extracted title strings |
| 4 | `clean()` | `titles.json` | `titles_clean.json`, an array of normalized titles with stop words removed |
| 5 | `count_words()` | `titles_clean.json` | `words.json`, a JSON object mapping words to integer counts |

The DAG is linear: each task starts only after its upstream task succeeds. The configured daily schedule is paused at creation by the local Compose environment, so an operator must unpause or manually trigger it in Airflow.

## Storage and deployment boundary

The Compose file bind-mounts `dags`, `logs`, and `plugins`; it does not define a persistent shared volume for the generated catalog and JSON artifacts. As implemented, intermediate files are written relative to the Airflow task's working directory. The checked-in `code_visualization/words.json` is a portable output snapshot; pipeline output must be copied there to update the chart.

For durable or distributed execution, define explicit artifact paths on a shared volume or object store. Do not rely on a worker container's local working directory as a persistent data layer.

## Runtime and dependency caveats

- The Compose configuration uses Apache Airflow `2.1.1`, PostgreSQL `13`, and a mutable `redis:latest` image. The Airflow runtime observed for this project uses Python 3.6.
- The task installs BeautifulSoup4 using `pip` at runtime. A production deployment should build and pin dependencies in a custom Airflow image.
- The legacy force-directed page loads D3 v3 and d3-tip; the packed bubble page uses D3 v7. They are separate examples with different APIs.
- The Docker Compose file is intended for local development and contains default credentials; do not expose this setup to an untrusted network.
