import geopandas as gpd
import folium
from pathlib import Path
import sys
import webbrowser
import time
import os

def create_map(file_path):
   buses = gpd.read_parquet(file_path)
   m = folium.Map(location=[40.7128, -74.0060], zoom_start=11)
   
   for idx, row in buses.iterrows():
       folium.CircleMarker(
           location=[row.latitude, row.longitude],
           radius=3,
           color='blue', 
           popup=f"Bus {row.vehicle_id}<br>Route {row.route_id}<br>Speed {row.speed:.1f}",
           fill=True
       ).add_to(m)

   timestamp = Path(file_path).stem
   folium.Rectangle(
       bounds=[[40.5, -74.3], [40.52, -74.0]],
       color="none",
       fill=False,
       popup=f"Timestamp: {timestamp}"
   ).add_to(m)
   
   return m

def main():
   if len(sys.argv) != 2:
       print("Usage: python script.py path/to/parquet/folder")
       sys.exit(1)

   folder_path = Path(sys.argv[1])
   parquet_files = sorted(list(folder_path.glob('*.geoparquet')))
   
   if not parquet_files:
       print(f"No parquet files found in {folder_path}")
       sys.exit(1)

   temp_dir = Path('temp_maps')
   temp_dir.mkdir(exist_ok=True)

   for file_path in parquet_files:
       print(f"Processing {file_path.name}...")
       m = create_map(file_path)
       html_path = temp_dir / f"{file_path.stem}.html"
       m.save(str(html_path))
       webbrowser.open(f'file://{html_path.absolute()}')
       time.sleep(2)  # Wait before opening next file

   print("\nAll maps generated. Press Ctrl+C to exit and clean up temp files.")
   try:
       while True:
           time.sleep(1)
   except KeyboardInterrupt:
       print("\nCleaning up...")
       for f in temp_dir.glob('*.html'):
           f.unlink()
       temp_dir.rmdir()

if __name__ == "__main__":
   main()