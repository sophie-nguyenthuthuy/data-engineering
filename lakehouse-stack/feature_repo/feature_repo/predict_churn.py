# Homework: Integrate Feast features into a simple churn-risk model
# Demonstrates reading online features and feeding them to a scikit-learn pipeline.

import os
import numpy as np
from feast import FeatureStore

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def predict_churn(customer_ids: list[str]) -> dict:
    store = FeatureStore(repo_path=".")

    entity_rows = [{"customer_id": cid} for cid in customer_ids]
    fv = store.get_online_features(
        features=[
            "customer_order_stats:customer_order_count",
            "customer_order_stats:avg_order_value",
            "customer_order_stats:total_revenue",
        ],
        entity_rows=entity_rows,
    ).to_dict()

    results = {}
    for i, cid in enumerate(fv["customer_id"]):
        order_count = fv["customer_order_count"][i] or 0
        avg_value = fv["avg_order_value"][i] or 0.0
        total_rev = fv["total_revenue"][i] or 0.0

        # Simple heuristic model (replace with trained sklearn model in production)
        # High churn risk: few orders AND low average value
        churn_score = max(0.0, 1.0 - (order_count / 20.0) - (avg_value / 300.0))
        churn_label = "HIGH" if churn_score > 0.7 else "MEDIUM" if churn_score > 0.4 else "LOW"

        results[cid] = {
            "customer_order_count": order_count,
            "avg_order_value": avg_value,
            "total_revenue": total_rev,
            "churn_score": round(churn_score, 4),
            "churn_risk": churn_label,
        }
    return results


if __name__ == "__main__":
    customers = [f"C{str(i).zfill(4)}" for i in range(1001, 1011)]
    predictions = predict_churn(customers)
    print(f"{'Customer':<10} {'Orders':>7} {'AvgVal':>9} {'Revenue':>10} {'Score':>7} {'Risk'}")
    print("-" * 55)
    for cid, r in predictions.items():
        print(
            f"{cid:<10} {r['customer_order_count']:>7} {r['avg_order_value']:>9.2f}"
            f" {r['total_revenue']:>10.2f} {r['churn_score']:>7.4f} {r['churn_risk']}"
        )
