import requests
import pandas as pd
import os
from dotenv import load_dotenv

def fetch_all_bus_stops(api_key):
    # Using https as http was returning 404
    url = "https://datamall2.mytransport.sg/ltaodataservice/BusStops"
    headers = {'AccountKey': api_key, 'accept': 'application/json'}
    all_stops = []
    skip = 0
    
    while True:
        params = {'$skip': skip}
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break
            
        data = response.json().get('value', [])
        if not data:
            break
            
        all_stops.extend(data)
        skip += 500
        print(f"Fetched {len(all_stops)} stops...")
        
    return all_stops

def main():
    load_dotenv()
    api_key = os.getenv('LTA_ACCOUNT_KEY')
    if not api_key or api_key == 'YOUR_ACTUAL_API_KEY_HERE':
        print("Error: Please set your LTA_ACCOUNT_KEY in the .env file.")
        return

    stops_data = fetch_all_bus_stops(api_key)
    
    if stops_data:
        df = pd.DataFrame(stops_data)
        # Extract 'BusStopCode' and 'Description'
        df_mapping = df[['BusStopCode', 'Description']]
        
        output_dir = 'data/mappings'
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = os.path.join(output_dir, 'bus_stop_mapping.parquet')
        df_mapping.to_parquet(output_path, engine='pyarrow', index=False)
        print(f"Saved {len(df_mapping)} bus stops to {output_path}")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
