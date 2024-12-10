import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions
from dagster_duckdb import DuckDBResource
from .assets.gtfs_rt_assets import gtfs_rt_feeds, gtfs_rt_data, gtfs_rt_database, gtfs_rt_schedule
from .api_utils import GTFSRTFeedManager, GTFSRTFeedManagerConfig

# Load environment variables
load_dotenv()

# Set up paths
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(REPO_ROOT, "data", "db")
DB_PATH = os.path.join(DB_DIR, "gtfs_rt.db")
CONFIG_PATH = os.path.join(REPO_ROOT, "feeds_config.yaml")

# Ensure database directory exists
os.makedirs(DB_DIR, exist_ok=True)

# Define Dagster definitions
defs = Definitions(
    assets=[gtfs_rt_feeds, gtfs_rt_data, gtfs_rt_database],
    schedules=[gtfs_rt_schedule],
    resources={
        "feed_manager": GTFSRTFeedManager(
            config=GTFSRTFeedManagerConfig(
                config_path=CONFIG_PATH
            )
        ),
        "duckdb": DuckDBResource(
            database=DB_PATH
        )
    },
)
