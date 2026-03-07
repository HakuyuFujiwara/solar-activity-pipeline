# Solar Activity Pipeline — Quick Reference

## Generate a Run
```bash
./generate_run.sh 76
```

Output: `/project2/erhodes_44/rcf-04/astro10/data/mdi/lnu/comparison/dailyactivityvalueshmirun76.dat`

## Common Examples
```bash
# Run 74 (2024-09-19 to 2024-11-29)
./generate_run.sh 74

# Run 75 (2024-11-30 to 2025-02-09)
./generate_run.sh 75

# Run 76 (2025-02-10 to 2025-04-22)
./generate_run.sh 76

# Any future run (dates auto-computed)
./generate_run.sh 77
```

## Check a Run's Dates Without Running
```bash
source .venv/bin/activate
python -c "from src.run_registry import get_run; r = get_run(77); print(f'{r.start_date} to {r.end_date}, JSOC {r.first_jsoc_day}')"
```

## Dry Run (fetch data but don't write to database)
```bash
source .venv/bin/activate
python -m src.pipeline --run 76 --dry-run
```

## Custom Date Range (not tied to a run)
```bash
source .venv/bin/activate
python -m src.pipeline --start-date 2025-01-01 --end-date 2025-03-31
```

## View Dashboard
```bash
source .venv/bin/activate
python -m streamlit run src/dashboard/app.py
```

## Run Tests
```bash
source .venv/bin/activate
pytest tests/ -v
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError` | `source .venv/bin/activate && pip install -e .` |
| `unable to open database file` | `mkdir -p data` |
| AAVSO returns 0 observations | Normal — bulletin not published yet for that month |
| Values differ slightly from old .dat | Normal — SILSO and MgII reprocess historical data |
