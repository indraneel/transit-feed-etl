from datetime import datetime
from typing import List, Dict, Any, Tuple
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sqlite3
from google.transit import gtfs_realtime_pb2
from dagster import (
    asset,
    AssetExecutionContext,
    RetryPolicy,
    DefaultScheduleStatus,
    MetadataValue,
    ScheduleDefinition,
    define_asset_job,
    Config,
    asset_check,
)
from ..api_utils import GTFSRTFeedManager

class GTFSRTFeedConfig(Config):
    """Configuration for GTFS-RT feed selection"""
    include_custom_feeds: bool = True

def parse_gtfs_rt_data(data: bytes, feed_id: str, timestamp: str) -> List[Dict]:
    """Parse GTFS-RT protobuf data and return a list of vehicle positions"""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)
    
    records = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle = entity.vehicle
            record = {
                'feed_id': feed_id,
                'timestamp': timestamp,
                'vehicle_id': vehicle.vehicle.id if vehicle.HasField('vehicle') else None,
                'latitude': vehicle.position.latitude if vehicle.HasField('position') else None,
                'longitude': vehicle.position.longitude if vehicle.HasField('position') else None,
                'speed': vehicle.position.speed if vehicle.HasField('position') else None,
                'bearing': vehicle.position.bearing if vehicle.HasField('position') else None,
                'trip_id': vehicle.trip.trip_id if vehicle.HasField('trip') else None,
                'route_id': vehicle.trip.route_id if vehicle.HasField('trip') else None,
                'current_stop_sequence': vehicle.current_stop_sequence if vehicle.HasField('current_stop_sequence') else None,
                'current_status': vehicle.current_status if vehicle.HasField('current_status') else None,
            }
            records.append(record)
    return records

@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=1),
    group_name="gtfs_rt"
)
def gtfs_rt_feeds(
    context: AssetExecutionContext,
    feed_manager: GTFSRTFeedManager,
) -> List[Dict[str, Any]]:
    """Fetch GTFS-RT feeds"""
    feeds = feed_manager.get_all_feeds()
    
    context.add_output_metadata(metadata={
        "feed_count": len(feeds),
        "feed_ids": [feed["id"] for feed in feeds],
        "timestamp": datetime.utcnow().isoformat(),
    })
    return feeds

@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=1),
    group_name="gtfs_rt"
)
def gtfs_rt_data(
    context: AssetExecutionContext,
    gtfs_rt_feeds: List[Dict[str, Any]],
    feed_manager: GTFSRTFeedManager,
) -> List[Dict]:
    """Fetch and parse real-time data for GTFS-RT feeds"""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    success_count = 0
    error_count = 0
    
    all_records = []
    for feed in gtfs_rt_feeds:
        feed_id = feed["id"]
        try:
            data = feed_manager.get_feed_data(feed)
            records = parse_gtfs_rt_data(data, feed_id, timestamp)
            all_records.extend(records)
            success_count += 1
            
        except Exception as e:
            context.log.error(f"Failed to process feed {feed_id}: {str(e)}")
            error_count += 1
    
    context.add_output_metadata(metadata={
        "success_count": success_count,
        "error_count": error_count,
        "record_count": len(all_records),
        "timestamp": timestamp,
    })
    
    return all_records

@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=1),
    group_name="gtfs_rt"
)
def gtfs_rt_database(
    context: AssetExecutionContext,
    gtfs_rt_data: List[Dict],
) -> None:
    """Store GTFS-RT data in geoparquet files and track metadata in SQLite"""
    if not gtfs_rt_data:
        context.log.warning("No records to store")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(gtfs_rt_data)
    
    # Convert to GeoDataFrame
    geometry = [
        Point(row['longitude'], row['latitude']) 
        if row['longitude'] is not None and row['latitude'] is not None 
        else None 
        for _, row in df.iterrows()
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # Create directories if they don't exist
    os.makedirs("data/db", exist_ok=True)
    os.makedirs("data/geoparquet", exist_ok=True)
    
    # Generate geoparquet file path based on current timestamp
    current_time = datetime.utcnow()
    geoparquet_dir = f"data/geoparquet/{current_time.strftime('%Y/%m/%d/%H')}"
    os.makedirs(geoparquet_dir, exist_ok=True)
    geoparquet_path = f"{geoparquet_dir}/{current_time.strftime('%M%S')}.geoparquet"
    
    # Write data to geoparquet file
    gdf.to_parquet(geoparquet_path, index=False)
    
    # Connect to SQLite database for metadata tracking
    with sqlite3.connect("data/db/gtfs_rt.db") as conn:
        # Create metadata table if it doesn't exist
        conn.execute("""
        CREATE TABLE IF NOT EXISTS geoparquet_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            record_count INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            bbox_minx REAL,
            bbox_miny REAL,
            bbox_maxx REAL,
            bbox_maxy REAL
        )
        """)
        
        # Calculate bounding box
        if len(gdf) > 0 and not all(g is None for g in gdf.geometry):
            total_bounds = gdf.total_bounds
            bbox = {
                'minx': total_bounds[0],
                'miny': total_bounds[1],
                'maxx': total_bounds[2],
                'maxy': total_bounds[3]
            }
        else:
            bbox = {'minx': None, 'miny': None, 'maxx': None, 'maxy': None}
        
        # Insert metadata about the geoparquet file
        conn.execute("""
        INSERT INTO geoparquet_files (
            file_path, timestamp, record_count, created_at,
            bbox_minx, bbox_miny, bbox_maxx, bbox_maxy
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            geoparquet_path,
            current_time.isoformat(),
            len(gdf),
            datetime.utcnow().isoformat(),
            bbox['minx'], bbox['miny'], bbox['maxx'], bbox['maxy']
        ))
        
        # Get total count of records across all geoparquet files
        total_count = conn.execute(
            "SELECT SUM(record_count) FROM geoparquet_files"
        ).fetchone()[0] or 0
    
    context.add_output_metadata(metadata={
        "geoparquet_file": geoparquet_path,
        "record_count": len(gdf),
        "total_records": total_count,
        "bbox": bbox if bbox['minx'] is not None else "No valid coordinates",
        "timestamp": current_time.isoformat(),
    })

@asset_check(asset=gtfs_rt_database)
def check_database_not_empty(context: AssetExecutionContext) -> None:
    """Check that geoparquet files exist and have recent data"""
    with sqlite3.connect("data/db/gtfs_rt.db") as conn:
        # Get total record count
        total_count = conn.execute(
            "SELECT SUM(record_count) FROM geoparquet_files"
        ).fetchone()[0] or 0
        
        # Get recent record count
        recent_count = conn.execute("""
            SELECT SUM(record_count) 
            FROM geoparquet_files 
            WHERE timestamp >= datetime('now', '-1 hour', 'localtime')
        """).fetchone()[0] or 0
        
        # Get latest geoparquet file
        latest_file = conn.execute("""
            SELECT file_path, timestamp, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy
            FROM geoparquet_files
            ORDER BY timestamp DESC
            LIMIT 1
        """).fetchone()
    
    # Verify latest geoparquet file exists
    if latest_file and not os.path.exists(latest_file[0]):
        raise AssertionError(f"Latest geoparquet file {latest_file[0]} not found")
    
    context.add_metadata(
        metadata={
            "total_records": MetadataValue.int(total_count),
            "records_last_hour": MetadataValue.int(recent_count),
            "latest_file": MetadataValue.text(latest_file[0] if latest_file else "None"),
            "latest_bbox": {
                "minx": latest_file[2],
                "miny": latest_file[3],
                "maxx": latest_file[4],
                "maxy": latest_file[5]
            } if latest_file and latest_file[2] is not None else "No valid coordinates",
            "check_timestamp": MetadataValue.text(datetime.utcnow().isoformat()),
        }
    )
    
    assert total_count > 0, "No records found"
    assert recent_count > 0, "No records added in the last hour"
    assert latest_file is not None, "No geoparquet files found"

# Define job and schedule
gtfs_rt_job = define_asset_job(
    name="gtfs_rt_job",
    selection=["gtfs_rt_feeds", "gtfs_rt_data", "gtfs_rt_database"],
)

gtfs_rt_schedule = ScheduleDefinition(
    job=gtfs_rt_job,
    cron_schedule="* * * * *",  # Every minute
    default_status=DefaultScheduleStatus.RUNNING,
)
