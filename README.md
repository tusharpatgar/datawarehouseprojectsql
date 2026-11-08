# Data Warehouse and Analytics Project (SQL Server)

An end-to-end SQL Server data warehouse practice project. It brings together CRM and ERP source data, loads a Bronze layer, transforms data into a Silver layer, and exposes analytics-ready Gold views using a star-schema model.

## Architecture

```text
CRM CSV files ─┐
               ├─> Bronze (raw tables)
ERP CSV files ─┘          |
                          v
                    Silver (cleaned,
                    standardized data)
                          |
                          v
                    Gold (dimensions
                    and fact view)
```

## Technology

- Microsoft SQL Server and T-SQL
- Stored procedures and bulk loading
- Bronze / Silver / Gold layered warehouse design
- Dimensional modeling and analytical views

## Repository structure

```text
datasets/
  source_crm/
  source_erp/
scripts/
  init_database.sql
  bronze/
    ddl_bronze.sql
    proc_load_bronze.sql
  silver/
    ddl_silver.sql
    proc_load.sql
  gold/
    gold_create.sql
README.md
LICENSE
```

## Prerequisites

- A SQL Server instance
- SQL Server Management Studio (SSMS) or another T-SQL client
- Permission to create databases, schemas, tables, and stored procedures
- Access for the SQL Server service account to read the CSV files

## Setup and execution

Run the scripts in this order from a development SQL Server instance:

1. `scripts/init_database.sql` — creates the `Datawarehouse` database and Bronze, Silver, and Gold schemas.
2. `scripts/bronze/ddl_bronze.sql` — creates raw Bronze tables.
3. `scripts/bronze/proc_load_bronze.sql` — creates and executes the Bronze load procedure.
4. `scripts/silver/ddl_silver.sql` — creates Silver tables.
5. `scripts/silver/proc_load.sql` — creates the Silver transformation procedure; execute `silver.load` after reviewing its behavior.
6. `scripts/gold/gold_create.sql` — creates the Gold dimension and fact views.

### Important: review before execution

- `scripts/init_database.sql` drops and recreates the `Datawarehouse` database if it already exists. This destroys existing data. Use a disposable development database and back up anything important first.
- `scripts/bronze/proc_load_bronze.sql` contains absolute Windows paths from the original development environment. Update every `BULK INSERT` path to a location readable by the SQL Server service account before running it.
- The Bronze load procedure truncates its target tables before loading. Re-running it replaces the current Bronze contents.
- Validate the source CSV files, SQL Server permissions, and expected column formats before loading.

## Data layers

- **Bronze:** raw CRM and ERP source tables.
- **Silver:** cleaned and standardized records, including date handling, duplicate handling, and business-rule transformations.
- **Gold:** customer and product dimensions plus a sales fact view for analytics.

## Analytical model

The Gold layer exposes:

- `gold.dim_customers`
- `gold.dim_products`
- `gold.fact_sales`

These views connect customer and product dimensions to sales measures for downstream reporting and analysis.

## Future improvements

- Replace machine-specific file paths with a documented environment configuration.
- Add automated data-quality assertions and reconciliation queries.
- Add incremental loading and execution logging.
- Add an ER diagram and sample analytical queries.

## License

See [LICENSE](LICENSE).
