# GTFS-RT Scraper

## Overview

GTFS-RT => Dagster (runs each minute) => GeoParquet + SQLite


## Project Structure

```
.
├── src/
│   └── gtfs_pipeline/        # Main pipeline package
│       ├── api_utils.py      # GTFS-RT API utilities
│       ├── definitions.py    # Pipeline definitions
│       └── assets/          # Dagster assets
├── data/                    # Data storage directory
│   ├── db/                 # SQLite database
│   └── geoparquet/        # Geoparquet files
├── read_gtfs_rt.py         # Map visualization script
├── feeds_config.yaml       # Feed configuration
└── dagster.yaml           # Dagster configuration
```

## Setup

0. Use `uv` and setup project, `.env` etc. 

1. Install dependencies:
```bash
pip install -e .
```

1. Configure your GTFS-RT feeds in `feeds_config.yaml`:
```yaml
custom_feeds:
  agency_name:
    url: "https://agency-gtfs-rt-feed-url"
    api_token: "your-api-token"  # Optional
    headers: {}  # Optional additional headers
```

3. Start the Dagster daemon:
```bash
dagster dev
```

## Usage

### Data Collection
The pipeline automatically collects data based on the configured schedule (default: every minute). You can monitor the pipeline through the Dagster UI.

### Visualizing Data
To visualize vehicle positions from collected data:

```bash
python read_gtfs_rt.py path/to/parquet/folder
```
## Data Storage

- **Geoparquet Files**: Vehicle position data is stored in Geoparquet format, organized by timestamp in the `data/geoparquet/` directory
- **SQLite Database**: Metadata about collected data is stored in `data/db/gtfs_rt.db`

