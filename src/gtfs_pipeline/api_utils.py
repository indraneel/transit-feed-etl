import os
import httpx
import yaml
from typing import List, Dict, Any, Optional
from dagster import ConfigurableResource, Config

class FeedConfig:
    """Configuration for a GTFS-RT feed"""
    def __init__(
        self,
        url: str,
        api_token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ):
        self.url = url
        self.api_token = api_token
        self.headers = headers or {}
        if api_token:
            self.headers["Authorization"] = f"Bearer {api_token}"

class GTFSRTFeedManagerConfig(Config):
    """Configuration for the GTFS-RT Feed Manager"""
    config_path: str

class GTFSRTFeedManager(ConfigurableResource):
    """Resource for managing GTFS-RT feeds"""
    config: GTFSRTFeedManagerConfig

    def _load_config(self) -> Dict[str, Any]:
        """Load the feed configuration from yaml file"""
        if not os.path.exists(self.config.config_path):
            return {}
        
        with open(self.config.config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config if config else {}
    
    def get_all_feeds(self) -> List[Dict[str, Any]]:
        """Get all configured feeds from the config file"""
        config = self._load_config()
        feeds = []
        
        # Process custom feeds
        custom_feeds = config.get("custom_feeds", {})
        for feed_id, feed_config in custom_feeds.items():
            feed_data = {
                "id": feed_id,
                "urls": {"realtime_vehicle_positions": feed_config["url"]},
                "source": "custom",
                "api_token": feed_config.get("api_token"),
                "headers": feed_config.get("headers", {})
            }
            feeds.append(feed_data)
        
        return feeds
    
    def get_feed_data(self, feed: Dict[str, Any]) -> bytes:
        """Get the GTFS-RT feed data from a feed configuration"""
        url = feed["urls"].get("realtime_vehicle_positions")
        if not url:
            raise ValueError(f"No valid URL found for feed {feed.get('id')}")
        
        headers = {}
        if feed.get("source") == "custom":
            headers = feed.get("headers", {})
            if feed.get("api_token"):
                headers["Authorization"] = f"Bearer {feed['api_token']}"
        
        try:
            response = httpx.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise Exception(f"Failed to fetch feed data from {url}: {str(e)}")
