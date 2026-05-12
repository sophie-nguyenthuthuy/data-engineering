-- Post-pipeline maintenance. Liquid clustering removes the need for ZORDER,
-- but OPTIMIZE still compacts small files, and VACUUM prunes old versions.

OPTIMIZE ${catalog}.gold.fct_sales;
OPTIMIZE ${catalog}.gold.dim_customer;
OPTIMIZE ${catalog}.gold.dim_product;

VACUUM ${catalog}.gold.fct_sales RETAIN 168 HOURS;
VACUUM ${catalog}.gold.dim_customer RETAIN 168 HOURS;
VACUUM ${catalog}.gold.dim_product RETAIN 168 HOURS;

ANALYZE TABLE ${catalog}.gold.fct_sales COMPUTE STATISTICS FOR ALL COLUMNS;
