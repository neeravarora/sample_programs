# Utilities to run Hive queries from Jupyter notebooks

`hive_query` runs Hive queries and loads results in a pandas DataFrame in multiple cluster infrastructures

`hive_create` creates tables in Hive from a pandas DataFrame in multiple cluster infrastructures

All utilities here require at a minimum the following Python environment:
```
- Python >= 3.6
- pandas >= 0.20
- lxml >= 4.0
- aws-cli >= 1.16 (CLI app)
- qds-sdk >= 1.9 (for Qubole only)
```
