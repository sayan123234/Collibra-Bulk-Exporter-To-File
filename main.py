import os
import json
import pandas as pd
import requests
import time
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dotenv import load_dotenv
from graphql_queries import QUERY_TYPES
from get_assetType_name import get_asset_type_name
from OauthAuth import oauth_bearer_token
from get_asset_type import get_available_asset_type

def setup_logging():
    """Configure logging with both file and console handlers."""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'collibra_exporter.log')

    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s'
    )

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

load_dotenv()

base_url = os.getenv('COLLIBRA_INSTANCE_URL')
file_path_to_save = os.getenv('FILE_SAVE_LOCATION', os.path.join(os.getcwd(), 'output'))
os.makedirs(file_path_to_save, exist_ok=True)
OUTPUT_FORMAT = os.getenv('OUTPUT_FORMAT', 'csv')

QUERY_LIMITS = {
    'main': 10000,
    'string': 490,
    'multi': 490,
    'numeric': 490,
    'boolean': 490,
    'outgoing': 490,
    'incoming': 490,
    'responsibilities': 490
}
def load_query_limits():
    """Load query limits from environment variables or use defaults."""
    env_limits = {}
    for query_type in QUERY_TYPES.keys():
        env_var = f'LIMIT_{query_type.upper()}'
        if limit_value := os.getenv(env_var):
            try:
                env_limits[query_type] = int(limit_value)
            except ValueError:
                logger.warning(f"Invalid limit value for {env_var}: {limit_value}. Using default.")
    return {**QUERY_LIMITS, **env_limits}

ACTIVE_QUERY_LIMITS = load_query_limits()

with open('Collibra_Asset_Type_Id_Manager.json', 'r') as file:
    data = json.load(file)

ASSET_TYPE_IDS = data['ids']

session = requests.Session()
session.headers.update({'Authorization': f'Bearer {oauth_bearer_token()}'})

def fetch_data_for_query_type(asset_type_id, query_type, paginate):
    """Fetch data for a specific query type using its specific limit."""
    try:
        query_func = QUERY_TYPES[query_type]
        limit = ACTIVE_QUERY_LIMITS[query_type]
        
        # Handle pagination parameter properly
        paginate_value = f'"{paginate}"' if paginate else 'null'
        query = query_func(asset_type_id, paginate_value, limit)
        
        variables = {'limit': limit}
        
        logger.debug(f"Sending GraphQL request for asset_type_id: {asset_type_id}, "
                    f"query_type: {query_type}, limit: {limit}")

        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        response = session.post(
            url=graphql_url,
            json={
                'query': query,
                'variables': variables
            }
        )
        response_time = time.time() - start_time
        logger.debug(f"GraphQL {query_type} request completed in {response_time:.2f} seconds")

        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            logger.error(f"GraphQL errors received for {query_type}: {data['errors']}")
            return None
            
        return data
    except Exception as error:
        logger.exception(f"Request failed for {query_type} query: {str(error)}")
        return None

def fetch_all_data_for_query_type(asset_type_id, query_type):
    """Fetch all pages of data for a specific query type."""
    all_results = []
    paginate = None
    batch_count = 0

    while True:
        batch_count += 1
        logger.debug(f"Fetching batch {batch_count} for query_type: {query_type}")
        
        response = fetch_data_for_query_type(asset_type_id, query_type, paginate)
        
        if not response or 'data' not in response or 'assets' not in response['data']:
            break

        assets = response['data']['assets']
        if not assets:
            break

        paginate = assets[-1]['id']
        all_results.extend(assets)
        logger.info(f"Batch {batch_count}: Fetched {len(assets)} assets for {query_type}")

    return all_results

def merge_asset_data(main_data, attribute_data):
    """Merge attribute data into main asset data using asset ID as key."""
    merged = {asset['id']: asset for asset in main_data}
    
    for attr_result in attribute_data:
        asset_id = attr_result['id']
        if asset_id in merged:
            attr_copy = attr_result.copy()
            attr_copy.pop('id', None)
            merged[asset_id].update(attr_copy)
    
    return list(merged.values())

def process_data(asset_type_id):
    """Process data by fetching all query types and merging results."""
    logger.info(f"Starting data processing for asset_type_id: {asset_type_id}")
    logger.debug(f"Using query limits: {ACTIVE_QUERY_LIMITS}")
    
    main_assets = fetch_all_data_for_query_type(asset_type_id, 'main')
    if not main_assets:
        logger.error("Failed to fetch main asset data")
        return []

    additional_query_types = ['string', 'multi', 'numeric', 'boolean', 'outgoing', 
                            'incoming', 'responsibilities']
    
    merged_assets = main_assets
    for query_type in additional_query_types:
        query_results = fetch_all_data_for_query_type(asset_type_id, query_type)
        if query_results:
            merged_assets = merge_asset_data(merged_assets, query_results)
        else:
            logger.warning(f"No data retrieved for query type: {query_type}")

    return merged_assets

def flatten_json(asset, asset_type_name):
    flattened = {
        f"Asset Id of {asset_type_name} ": asset['id'],
        f"{asset_type_name} Full Name": asset['fullName'],
        f"{asset_type_name} Name": asset['displayName'],
        "Asset Type": asset['type']['name'],
        "Status": asset['status']['name'],
        f"Domain of {asset_type_name}": asset['domain']['name'],
        f"Community of {asset_type_name}": asset['domain']['parent']['name'] if asset['domain']['parent'] else None,
        f"{asset_type_name} modified on": asset['modifiedOn'],
        f"{asset_type_name} last modified By": asset['modifiedBy']['fullName'],
        f"{asset_type_name} created on": asset['createdOn'],
        f"{asset_type_name} created By": asset['createdBy']['fullName'],
    }

    responsibilities = asset.get('responsibilities', [])
    if responsibilities:
        flattened[f"User Role Against {asset_type_name}"] = ', '.join(r['role']['name'] for r in responsibilities if 'role' in r)
        flattened[f"User Name Against {asset_type_name}"] = ', '.join(r['user']['fullName'] for r in responsibilities if 'user' in r)
        flattened[f"User Email Against {asset_type_name}"] = ', '.join(r['user']['email'] for r in responsibilities if 'user' in r)

    string_attrs = defaultdict(list)

    for attr_type in ['multiValueAttributes', 'stringAttributes', 'numericAttributes', 'dateAttributes', 'booleanAttributes']:
        for attr in asset.get(attr_type, []):
            attr_name = attr['type']['name']
            if attr_type == 'multiValueAttributes':
                flattened[attr_name] = ', '.join(attr['stringValues'])
            elif attr_type == 'stringAttributes':
                string_attrs[attr_name].append(attr['stringValue'].strip())
            else:
                value_key = f"{attr_type[:-10]}Value"
                flattened[attr_name] = attr[value_key]

    for attr_name, values in string_attrs.items():
        if len(set(values)) > 1:
            flattened[attr_name] = ', '.join(set(values))
        else:
            flattened[attr_name] = values[0]

    relation_types = defaultdict(list)
    relation_ids = defaultdict(list)
    for relation_direction in ['outgoingRelations', 'incomingRelations']:
        for relation in asset.get(relation_direction, []):
            role_or_corole = 'role' if relation_direction == 'outgoingRelations' else 'corole'
            role_type = relation['type'].get(role_or_corole, '')
            target_or_source = 'target' if relation_direction == 'outgoingRelations' else 'source'
            
            if relation_direction == 'outgoingRelations':
                rel_type = f"{relation[target_or_source]['type']['name']} {role_type} {asset_type_name}"
            else:
                rel_type = f"{asset_type_name} {role_type} {relation[target_or_source]['type']['name']}"
            
            display_name = relation[target_or_source].get('displayName', '')
            asset_id = relation[target_or_source].get('id', '')
            
            if display_name:
                relation_types[rel_type].append(display_name.strip())
                relation_ids[rel_type].append(asset_id)

    for rel_type, values in relation_types.items():
        flattened[rel_type] = ', '.join(values)
        flattened[f"{rel_type} Asset IDs"] = ', '.join(str(id) for id in relation_ids[rel_type])

    return flattened

def save_data(data, file_name, format='excel'):
    """Save the processed data to a file."""
    logger.info(f"Starting to save data with format: {format}")
    start_time = time.time()

    try:
        file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        full_file_path = os.path.join(file_path_to_save, file_name)
        
        df = pd.DataFrame(data)
        logger.debug(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")

        if format == 'json':
            output_file = f'{full_file_path}.json'
            df.to_json(output_file, orient='records', indent=2)
        elif format == 'csv':
            output_file = f'{full_file_path}.csv'
            df.to_csv(output_file, index=False)
        else:
            output_file = f'{full_file_path}.xlsx'
            df.to_excel(output_file, index=False)

        logger.info(f"Successfully saved data to {output_file}")
        return output_file

    except Exception as e:
        logger.exception(f"Failed to save data: {str(e)}")
        raise

def process_asset_type(asset_type_id):
    """Process a single asset type."""
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(asset_type_id)

    if all_assets:
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        output_filename = f"{asset_type_name}"
        output_file = save_data(flattened_assets, output_filename, OUTPUT_FORMAT)

        elapsed_time = time.time() - start_time
        logger.info(f"Completed processing {asset_type_name} in {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logger.critical(f"No data to save for {asset_type_name}")
        return 0

def main():
    """Main execution function."""
    logger.info("Starting Collibra Bulk Exporter")
    logger.info(f"Output format: {OUTPUT_FORMAT}")
    logger.info(f"Number of asset types to process: {len(ASSET_TYPE_IDS)}")

    total_start_time = time.time()
    successful_exports = 0
    failed_exports = 0
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_asset = {executor.submit(process_asset_type, asset_type_id): asset_type_id 
                          for asset_type_id in ASSET_TYPE_IDS}
        
        for future in as_completed(future_to_asset):
            asset_type_id = future_to_asset[future]
            try:
                elapsed_time = future.result()
                if elapsed_time:
                    successful_exports += 1
                else:
                    failed_exports += 1
            except Exception as e:
                failed_exports += 1
                logger.exception(f"Error processing asset type ID {asset_type_id}")
    
    total_time = time.time() - total_start_time
    
    logger.info(f"\nExport Summary:")
    logger.info(f"Total asset types processed: {len(ASSET_TYPE_IDS)}")
    logger.info(f"Successful exports: {successful_exports}")
    logger.info(f"Failed exports: {failed_exports}")
    logger.info(f"Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error in main program")
        raise