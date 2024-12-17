import os
import json
import pandas as pd
import requests
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dotenv import load_dotenv
from graphql_query import get_query
from get_assetType_name import get_asset_type_name
from OauthAuth import oauth_bearer_token
from get_asset_type import get_available_asset_type

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Load configuration from environment
base_url = os.getenv('COLLIBRA_INSTANCE_URL')
file_path_to_save = os.getenv('FILE_SAVE_LOCATION', os.path.join(os.getcwd(), 'output'))

# Ensure the output directory exists
os.makedirs(file_path_to_save, exist_ok=True)

# Define output format as a variable with a default value
OUTPUT_FORMAT = os.getenv('OUTPUT_FORMAT', 'csv')  # Can be 'json', 'csv', or 'excel'

with open('Collibra_Asset_Type_Id_Manager.json', 'r') as file:
    data = json.load(file)

ASSET_TYPE_IDS = data['ids']

#######!!!!WARNING!!!Uncomment only if you want to export all the asset types##########

# asset_types_response = get_available_asset_type()
# if asset_types_response and 'results' in asset_types_response:
#     ASSET_TYPE_IDS = [asset['id'] for asset in asset_types_response['results']]
#     logging.info(f"Retrieved {len(ASSET_TYPE_IDS)} asset type IDs")
# else:
#     logging.error("Failed to retrieve asset type IDs. Exiting.")
#     ASSET_TYPE_IDS = []
#     exit(1)
    
###############################################

session = requests.Session()
session.headers.update({'Authorization': f'Bearer {oauth_bearer_token()}'})

def fetch_data(asset_type_id, paginate, limit):
    try:
        query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null')
        variables = {'limit': limit}
        logging.info(f"Sending request with variables: {variables} and paginate: {paginate}")

        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        response = session.post(
            url=graphql_url,
            json={
                'query': query,
                'variables': variables
            }
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as error:
        logging.error(f'Error fetching data: {error}')
        return None

def process_data(asset_type_id, limit=94):
    all_assets = []
    paginate = None

    while True:
        object_response = fetch_data(asset_type_id, paginate, limit)

        if object_response and 'data' in object_response and 'assets' in object_response['data']:
            assets = object_response['data']['assets']

            if not assets:
                logging.info("No more assets to fetch.")
                break

            paginate = assets[-1]['id']

            logging.info(f"Fetched {len(assets)} assets")
            all_assets.extend(assets)
        else:
            logging.warning('No assets found or there was an error fetching data.')
            break

    if not all_assets:
        logging.warning("No data was fetched.")

    logging.info(f"Total assets fetched: {len(all_assets)}")
    return all_assets

def flatten_json(asset, asset_type_name):
    flattened = {
        f"{asset_type_name} last Modified On": asset['modifiedOn'],
        f"UUID of {asset_type_name}": asset['id'],
        f"{asset_type_name} Full Name": asset['fullName'],
        f"{asset_type_name} Name": asset['displayName'],
        "Asset Type": asset['type']['name'],
        "Status": asset['status']['name'],
        f"Domain of {asset_type_name}": asset['domain']['name'],
        f"Community of {asset_type_name}": asset['domain']['parent']['name'] if asset['domain']['parent'] else None,
    }

    responsibilities = asset.get('responsibilities', [])
    if responsibilities:
        flattened[f"User Role Against {asset_type_name}"] = ', '.join(r['role']['name'] for r in responsibilities if 'role' in r)
        flattened[f"User Name Against {asset_type_name}"] = ', '.join(r['user']['fullName'] for r in responsibilities if 'user' in r)
        flattened[f"User Email Against {asset_type_name}"] = ', '.join(r['user']['email'] for r in responsibilities if 'user' in r)

    # Temporary storage for string attributes
    string_attrs = defaultdict(list)

    for attr_type in ['multiValueAttributes', 'stringAttributes', 'numericAttributes', 'dateAttributes', 'booleanAttributes']:
        for attr in asset.get(attr_type, []):
            attr_name = attr['type']['name']
            if attr_type == 'multiValueAttributes':
                flattened[attr_name] = ', '.join(attr['stringValues'])
            elif attr_type == 'stringAttributes':
                # Collect string attributes
                string_attrs[attr_name].append(attr['stringValue'].strip())
            else:
                value_key = f"{attr_type[:-10]}Value"
                flattened[attr_name] = attr[value_key]

    # Process collected string attributes
    for attr_name, values in string_attrs.items():
        if len(set(values)) > 1:
            flattened[attr_name] = ', '.join(set(values))
        else:
            flattened[attr_name] = values[0]

    relation_types = defaultdict(list)
    for relation_direction in ['outgoingRelations', 'incomingRelations']:
        for relation in asset.get(relation_direction, []):
            role_or_corole = 'role' if relation_direction == 'outgoingRelations' else 'corole'
            role_type = relation['type'].get(role_or_corole, '')
            target_or_source = 'target' if relation_direction == 'outgoingRelations' else 'source'
            
            if relation_direction == 'outgoingRelations':
                rel_type = f"{asset_type_name} {role_type} {relation[target_or_source]['type']['name']}"
            else:
                rel_type = f"{relation[target_or_source]['type']['name']} {role_type} {asset_type_name}"
            
            display_name = relation[target_or_source].get('displayName', '')
            if display_name:
                relation_types[rel_type].append(display_name.strip())

    flattened.update({rel_type: ', '.join(values) for rel_type, values in relation_types.items()})

    return flattened

def save_data(data, file_name, format='excel'):
    # Remove any invalid filename characters
    file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
    
    # Join the file path with the filename
    full_file_path = os.path.join(file_path_to_save, file_name)
    
    # Validate output format
    valid_formats = ['json', 'csv', 'excel']
    if format not in valid_formats:
        logging.warning(f"Invalid format '{format}'. Using default format (Excel).")
        format = 'excel'
    
    df = pd.DataFrame(data)
    
    if format == 'json':
        json_file = f'{full_file_path}.json'
        df.to_json(json_file, orient='records', indent=2)
        logging.info(f"Data saved as JSON in {json_file}")
        return json_file
    elif format == 'csv':
        csv_file = f'{full_file_path}.csv'
        df.to_csv(csv_file, index=False)
        logging.info(f"Data saved as CSV in {csv_file}")
        return csv_file
    else:  # excel
        excel_file = f'{full_file_path}.xlsx'
        df.to_excel(excel_file, index=False)
        logging.info(f"Data saved as Excel in {excel_file}")
        return excel_file

def process_asset_type(asset_type_id):
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logging.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(asset_type_id)

    if all_assets:
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        output_filename = f"{asset_type_name}"
        output_file = save_data(flattened_assets, output_filename, OUTPUT_FORMAT)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logging.info(f"Time taken to process {asset_type_name}: {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logging.critical(f"No data to save")
        return 0

def main():
    total_start_time = time.time()
    
    total_elapsed_time = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_asset = {executor.submit(process_asset_type, asset_type_id): asset_type_id for asset_type_id in ASSET_TYPE_IDS}
        for future in as_completed(future_to_asset):
            elapsed_time = future.result()
            if elapsed_time:
                total_elapsed_time += elapsed_time
    
    total_end_time = time.time()
    total_program_time = total_end_time - total_start_time
    
    logging.info(f"\nTotal time taken to process all asset types: {total_elapsed_time:.2f} seconds")
    logging.info(f"Total program execution time: {total_program_time:.2f} seconds")

if __name__ == "__main__":
    main()