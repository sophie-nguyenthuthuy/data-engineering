import pandas as pd
import great_expectations as gx

# Read sample data
orders = pd.read_csv("orders.csv")

# File-based context — creates ./gx/ on first run.
# Switched from mode="ephemeral" so validation results persist
# and Data Docs render with real content.
context = gx.get_context()

# Datasource + asset (idempotent across runs).
datasource = context.sources.add_or_update_pandas(name="bootcamp_source")
asset_name = "orders_asset"
try:
    asset = datasource.get_asset(asset_name)
except LookupError:
    asset = datasource.add_dataframe_asset(name=asset_name)
batch_request = asset.build_batch_request(dataframe=orders)

# Suite (idempotent).
suite_name = "orders_suite"
context.add_or_update_expectation_suite(suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

# Show full list of offending rows (default BASIC returns counts only).
validator.set_default_expectation_argument("result_format", "COMPLETE")

# Task A2. Add common expectations
# Schema of orders.csv: order_id, order_timestamp, quantity, unit_price, order_status, payment_method
validator.expect_column_values_to_not_be_null("order_id")               # completeness
validator.expect_column_values_to_be_unique("order_id")                 # accuracy / no dupes
validator.expect_column_values_to_not_be_null("payment_method")         # completeness
validator.expect_column_values_to_be_between("quantity", min_value=1, max_value=20)        # validity
validator.expect_column_values_to_be_between("unit_price", min_value=0, max_value=1000)    # validity
validator.expect_column_values_to_be_in_set(
    "order_status",
    ["COMPLETED", "PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]
)                                                                       # consistency
validator.expect_column_values_to_be_in_set(
    "payment_method",
    ["CREDIT_CARD", "PAYPAL", "DEBIT_CARD", "BANK_TRANSFER", "CASH"]
)                                                                       # consistency
validator.expect_table_row_count_to_be_between(min_value=1, max_value=100000)  # size

# Persist the suite so the Data Docs site references the same definitions.
validator.save_expectation_suite(discard_failed_expectations=False)

# Task A3. Run validation
# Run via Checkpoint so the result is saved to the validations store and
# Data Docs pick it up. We still call validator.validate() for the
# console summary because it returns a richer object for our print loop.
result = validator.validate()
print("Success:", result["success"])
print("Validation Results Detailed:")

for res in result["results"]:
    cfg = res["expectation_config"]
    target = cfg["kwargs"].get("column", "<table>")
    label = f"{cfg['expectation_type']} on {target}"
    if res["success"]:
        print(f"PASSED: {label}")
        continue
    r = res["result"]
    count = r.get("unexpected_count", "?")
    pct = r.get("unexpected_percent", 0)
    sample = (r.get("partial_unexpected_list")
              or r.get("unexpected_list")
              or [])
    print(f"FAILED: {label}")
    print(f"  --> {count} bad rows ({pct:.2f}%); sample: {sample[:5]}")

# Also run through a Checkpoint to persist result for Data Docs.
checkpoint = context.add_or_update_checkpoint(
    name="orders_checkpoint",
    validator=validator,
)
checkpoint.run()

# Build + auto-open the HTML report at gx/uncommitted/data_docs/local_site/index.html
context.build_data_docs()
context.open_data_docs()
