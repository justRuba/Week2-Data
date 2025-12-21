import sys
from pathlib import Path
from datetime import datetime, timezone
import logging
from src.bootcamp_data.config import make_paths
from src.bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from src.bootcamp_data.transforms import enforce_schema
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from src.bootcamp_data.config import make_paths
paths = make_paths(Path(".").resolve())
print(paths.raw)
