# Facility‑level Lithium Supply‑Chain Mapping

This repository accompanies the article

> **“Is the concentration of battery manufacturing in China an inevitable risk to global lithium‑ion battery supply?”**  
> *Luke Cullen, Baptiste Andrieu, Scott Jeen, André Cabrera Serrenho, Jonathan Cullen*  
> Department of Engineering, University of Cambridge — May 2025

We build the first **facility‑level map of the global lithium supply chain**, tracing material flows from mines to battery plants.  
Although 84 % of cells are assembled in China, our network analysis shows that the most critical disruption points are distributed across Chile, Australia, China and Japan.

---

## Repository layout

```
.
├── preprocessing/          # Clean Benchmark workbooks & build graph tables
│   ├── benchmark_formatting.py
│   ├── create_partnership_matches.py
│   ├── benchmark_to_graphElements.py
│   └── edges.py
│
├── utils/                  # Name normalisation, GCS helpers, metadata, …
├── figures/                # Notebooks that generate every article figure
│   └── color_map_temp.json
│
├── run_preprocessing.py    # One‑shot launcher for the four preprocessing steps
├── pyproject.toml          # All dependencies; uv.lock is generated automatically
└── .pre-commit-config.yaml # black, isort, flake8, …
```

---

## Setting‑up a development environment with **uv**

The project uses **[uv](https://github.com/astral-sh/uv)** — a fast, drop‑in replacement for pip + virtualenv + poetry.

### 1 ) Install `uv` (one time)

*macOS / Linux*

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

*Windows (PowerShell)*

```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2 ) Create the virtual environment and install all dependencies

```bash
uv sync
```

`uv` reads **pyproject.toml**, creates `.venv/` in the project root, writes a deterministic **uv.lock**, and downloads every pinned package.

### 3 ) (Optional) activate the venv

Your IDE will usually pick up `.venv` automatically.  
If you need to activate it manually:

```bash
# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 4 ) Add new packages

```bash
uv add matplotlib
```

The command updates `pyproject.toml` **and** regenerates `uv.lock`. Commit both files.

### 5 ) Install the pre‑commit hooks (code style & linting)

```bash
pre-commit install
```

---

## Running the pipeline

```bash
python run_preprocessing.py
```

The script runs, in order:

1. **benchmark_formatting** – harmonise raw Benchmark spreadsheets  
2. **create_partnership_matches** – merge GPT‑assisted fuzzy matches  
3. **benchmark_to_graphElements** – write canonical node / edge tables  
4. **edges** – allocate material flows and save final edge lists  

When it finishes you can open any notebook under `figures/` and **Run All** to recreate the paper’s plots (Sankey, tree‑Sankey, node‑focus panels, etc.).

---

## Proprietary data warning

The scripts pull confidential Benchmark Minerals Intelligence spreadsheets from a private Google Cloud Storage bucket.  
Without a Benchmark licence and GCS credentials the pipeline will stop at the download step. We publish the code for transparency; full replication requires access to the original data.

---

## Citing

If you use the code or methodology, please cite the article above and reference this repository.

---

## Licence

Code: MIT.  
Data: © Benchmark Minerals Intelligence, used under licence; not redistributed here.
