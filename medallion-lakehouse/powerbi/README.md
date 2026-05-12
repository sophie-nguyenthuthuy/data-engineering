# Power BI semantic model — SalesModel

DirectLake semantic model over the `gold` schema of the medallion catalog. No scheduled refresh, no data copy — Power BI reads Delta files from ADLS Gen2 directly.

## Layout

```
SalesModel.SemanticModel/
├── database.tmdl                         Model, roles, relationships
└── definition/tables/
    ├── fct_sales.tmdl                    Fact + measures (Total Sales, YoY, AOV)
    ├── dim_customer.tmdl                 SCD2 "current" view
    ├── dim_product.tmdl
    └── dim_date.tmdl                     Marked as time table
```

## Publish

The `SalesModel.SemanticModel` folder is a TMDL project compatible with [Tabular Editor 3](https://tabulareditor.com/) and Power BI Desktop's TMDL mode. To deploy to a Fabric workspace:

```bash
# via fabric-cli
fab set -u /myworkspace/SalesModel.SemanticModel -q "connection.connectionString=abfss://gold@<storage>.dfs.core.windows.net"
fab import -u /myworkspace -i ./SalesModel.SemanticModel
```

Or use the Fabric Git integration: commit this folder to the workspace-connected branch and Fabric will sync automatically.

## DirectLake prerequisites

1. The Power BI / Fabric workspace must be in a capacity with DirectLake enabled (F64+ or P1+).
2. The service principal running the semantic model must have `SELECT` on `<catalog>.gold.*`.
3. The SQL analytics endpoint of the Databricks catalog must be exposed to the Fabric workspace via a shortcut or mirrored database.

## Roles

- **Viewer** — reads all tables except the `email` column on `dim_customer`.
- **PII_Reader** — unrestricted read. Assigned only to the support + ops AD groups.
