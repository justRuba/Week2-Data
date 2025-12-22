import sys
from pathlib import Path
from datetime import datetime, timezone
import logging
import json
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema, normalize_text, apply_mapping, dedupe_keep_latest
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key, assert_in_range, add_missing_flags,missingness_report

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

def main() -> None:
    p = make_paths(ROOT)

    orders = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")

    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    orders = enforce_schema(orders)

    assert_in_range(orders["amount"], lo=0, name="amount")
    assert_in_range(orders["quantity"], lo=1, name="quantity")

    orders["status"] = normalize_text(orders["status"])
    orders["status"] = apply_mapping(
        orders["status"],
        {"paid": "paid", "refund": "refund", "refunded": "refund"}
    )

    orders = dedupe_keep_latest(orders, ["order_id"], "created_at")
    assert_unique_key(orders, "order_id")

    orders = add_missing_flags(orders, ["amount", "quantity"])

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(exist_ok=True, parents=True)
    missing_report = missingness_report(orders)
    missing_report.to_csv(reports_dir / "missingness_orders.csv", index=True)
    log.info("Missingness report saved to: %s", reports_dir / "missingness_orders.csv")

    out_orders = p.processed / "orders_clean.parquet"
    out_users = p.processed / "users.parquet"
    write_parquet(orders, out_orders)
    write_parquet(users, out_users)
    log.info("Cleaned orders saved to: %s", out_orders)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows": {"orders": int(len(orders)), "users": int(len(users))},
        "outputs": {"orders": str(out_orders), "users": str(out_users)},
    }
    meta_path = p.processed / "_run_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("Run metadata saved at: %s", meta_path)

    log.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
