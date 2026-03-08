# ETL-Pipeline


Project Structure

extract/ – Data extraction from CSV
transform/ – Data cleaning and enrichment
load/ – Loading into PostgreSQL
warehouse/ – Database config, schema, setup
queries/ – Analytics SQL queries
data_sources/ – Sample CSV files
logs/ – Pipeline execution logs
main_pipeline.py – Orchestrator script
quickstart.sh – One‑command setup & run
Requirements

Python 3.8+
PostgreSQL (local or remote)
Packages: pandas, SQLAlchemy, psycopg2-binary (see requirements.txt)
