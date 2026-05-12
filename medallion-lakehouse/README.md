# medallion-lakehouse

Production-grade Azure Databricks lakehouse: bronze/silver/gold on Delta Live Tables, governed by Unity Catalog, served to Power BI via DirectLake. Infrastructure as Terraform, pipelines as Databricks Asset Bundles, CI/CD on GitHub Actions.

A better-built version of the common "Databricks + ADF + Synapse + Power BI" medallion reference. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the why.

## Layout

```
.
├── docs/ARCHITECTURE.md       Design doc — read this first
├── infra/                     Terraform: ADLS Gen2, Key Vault, workspace, UC
│   └── modules/
├── databricks/                Asset Bundle: DLT pipelines, jobs, tests
│   ├── databricks.yml         Bundle config (dev/staging/prod targets)
│   ├── resources/             Pipeline + job definitions
│   ├── src/                   bronze / silver / gold / common
│   └── tests/                 pytest for pure functions
├── powerbi/                   TMDL semantic model (DirectLake)
├── sample_data/               Synthetic e-commerce dataset
├── scripts/                   Data generator + helper scripts
└── .github/workflows/         validate + deploy
```

## Prerequisites

- Terraform ≥ 1.6
- Databricks CLI ≥ 0.230 (`brew install databricks/tap/databricks`)
- An Azure subscription with Owner or Contributor + User Access Administrator
- `az login` and a service principal for CI (see `docs/ARCHITECTURE.md#security`)

## Bootstrap

```bash
# 1. Provision infra
cd infra
cp terraform.tfvars.example terraform.tfvars   # edit
terraform init
terraform apply

# 2. Deploy pipelines to dev
cd ../databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev

# 3. Seed sample data
python ../scripts/generate_sample_data.py --rows 100000 --out ../sample_data
databricks fs cp -r ../sample_data/ dbfs:/Volumes/dev_sales/bronze/landing/

# 4. Run the pipeline
databricks bundle run sales_medallion -t dev
```

## Promoting to prod

```bash
databricks bundle deploy -t staging    # manual from main
git tag v1.2.0 && git push --tags      # CI deploys to prod
```

## Running tests

```bash
cd databricks
pip install -r requirements-dev.txt
pytest tests/
```

## Sample dataset

`scripts/generate_sample_data.py` produces a synthetic B2C e-commerce dataset:
- `customers.csv` — SCD2 candidates (address changes over time)
- `products.csv` — slowly-changing catalog
- `orders.csv` — fact source, ~100k rows/day
- `order_items.csv` — line items

The gold star schema models these as `dim_customer` (SCD2), `dim_product` (SCD1), `dim_date`, and `fct_sales`.

## License

MIT.
# medallion-lakehouse
