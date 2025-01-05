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
from graphql_query import get_query
from get_assetType_name import get_asset_type_name
from OauthAuth import oauth_bearer_token
from get_asset_type import get_available_asset_type

def setup_logging():
    """Configure logging with both file and console handlers."""
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'collibra_exporter.log')

    # Create formatters and handlers
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s'
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5  # 10MB per file, keep 5 backups
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # Root logger configuration
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Setup logging first
logger = setup_logging()

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

def fetch_data(asset_type_id, paginate, limit, nested_offset=0, nested_limit=50):
    try:
        query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null', nested_offset, nested_limit)
        variables = {'limit': limit}
        logger.debug(f"Sending GraphQL request for asset_type_id: {asset_type_id}, paginate: {paginate}, nested_offset: {nested_offset}")

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
        logger.debug(f"GraphQL request completed in {response_time:.2f} seconds")

        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            logger.error(f"GraphQL errors received: {data['errors']}")
            return None
            
        return data
    except requests.RequestException as error:
        logger.exception(f"Request failed for asset_type_id {asset_type_id}: {str(error)}")
        return None
    except json.JSONDecodeError as error:
        logger.exception(f"Failed to parse JSON response: {str(error)}")
        return None

def process_data(asset_type_id, limit=94, nested_limit=50):
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info("="*60)
    logger.info(f"Starting data processing for asset type: {asset_type_name} (ID: {asset_type_id})")
    logger.info(f"Configuration - Batch Size: {limit}, Nested Limit: {nested_limit}")
    logger.info("="*60)
    
    all_assets = []
    paginate = None
    batch_count = 0
    start_time = time.time()

    while True:
        batch_count += 1
        batch_start_time = time.time()
        logger.info(f"\n[Batch {batch_count}] Starting new batch for {asset_type_name}")
        logger.debug(f"[Batch {batch_count}] Pagination token: {paginate}")
        
        # Fetch initial data
        object_response = fetch_data(asset_type_id, paginate, limit, 0, nested_limit)

        if not object_response:
            logger.error(f"[Batch {batch_count}] [ERROR] Failed to fetch data for {asset_type_name}")
            # Log the full response for debugging
            logger.error(f"Response: {object_response}")
            break

        if 'data' in object_response and 'assets' in object_response['data']:
            assets = object_response['data']['assets']

            if not assets:
                logger.info(f"[Batch {batch_count}] [DONE] No more assets to fetch for {asset_type_name}")
                break

            logger.info(f"[Batch {batch_count}] Processing {len(assets)} assets")
            
            # For each asset, fetch all nested data with pagination
            for idx, asset in enumerate(assets, 1):
                asset_start_time = time.time()
                asset_id = asset.get('id', 'Unknown ID')
                asset_name = asset.get('displayName', 'Unknown Name')
                
                logger.info(f"\n[Batch {batch_count}][Asset {idx}/{len(assets)}] " +
                          f"Processing asset: {asset_name} (ID: {asset_id})")
                
                # Log the initial asset data structure
                logger.debug(f"[DEBUG] Initial asset data keys: {list(asset.keys())}")
                
                complete_asset = asset.copy()
                
                # Lists to store all nested data
                nested_fields = [
                    'stringAttributes', 
                    'multiValueAttributes', 
                    'numericAttributes', 
                    'dateAttributes', 
                    'booleanAttributes', 
                    'outgoingRelations', 
                    'incomingRelations', 
                    'responsibilities'
                ]

                missing_fields = []
                empty_fields = []
                
                # Initialize nested fields if they don't exist
                for field in nested_fields:
                    field_start_time = time.time()
                    
                    # Check if field exists in initial response
                    if field not in complete_asset:
                        missing_fields.append(field)
                        logger.warning(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                    f"[WARNING] Field missing in initial response for asset {asset_id}")
                        complete_asset[field] = []
                        continue
                        
                    initial_data = complete_asset[field]
                    
                    # Check if field is empty in initial response
                    if not initial_data:
                        empty_fields.append(field)
                        logger.warning(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                    f"[WARNING] Field is empty in initial response for asset {asset_id}")
                    
                    logger.debug(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                               f"Initial count: {len(initial_data)}")
                    
                    # Only continue pagination if we got exactly nested_limit items
                    if len(initial_data) == nested_limit:
                        logger.info(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                  f"Found {nested_limit} items, checking for more...")
                        nested_offset = nested_limit
                        total_items = len(initial_data)
                        
                        while True:
                            nested_response = fetch_data(
                                asset_type_id, 
                                asset_id, 
                                1, 
                                nested_offset, 
                                nested_limit
                            )
                            
                            # Log the nested query response for debugging
                            if nested_response is None:
                                logger.error(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                         f"[ERROR] Nested query returned None for asset {asset_id}")
                                break
                                
                            if 'errors' in nested_response:
                                logger.error(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                         f"[ERROR] GraphQL errors in nested query for asset {asset_id}: " +
                                         f"{nested_response['errors']}")
                                break
                            
                            if not nested_response or \
                               'data' not in nested_response or \
                               'assets' not in nested_response['data'] or \
                               not nested_response['data']['assets']:
                                logger.warning(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                            f"[WARNING] Failed to fetch additional data at offset {nested_offset}")
                                break
                            
                            nested_data = nested_response['data']['assets'][0].get(field, [])
                            
                            if not nested_data or len(nested_data) < nested_limit:
                                if nested_data:
                                    total_items += len(nested_data)
                                    complete_asset[field].extend(nested_data)
                                logger.info(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                          f"[DONE] Reached end with {total_items} total items")
                                break
                            
                            total_items += len(nested_data)
                            complete_asset[field].extend(nested_data)
                            nested_offset += nested_limit
                            logger.debug(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                                      f"Retrieved {len(nested_data)} more items at offset {nested_offset - nested_limit}")
                    
                    field_time = time.time() - field_start_time
                    logger.info(f"[Batch {batch_count}][Asset {idx}][{field}] " +
                              f"[DONE] Completed in {field_time:.2f}s with {len(complete_asset[field])} total items")

                # Log summary of missing or empty fields
                if missing_fields or empty_fields:
                    logger.warning(f"\n[Batch {batch_count}][Asset {idx}] Data completeness report for asset {asset_id}:")
                    if missing_fields:
                        logger.warning(f"Missing fields: {', '.join(missing_fields)}")
                    if empty_fields:
                        logger.warning(f"Empty fields: {', '.join(empty_fields)}")

                all_assets.append(complete_asset)
                asset_time = time.time() - asset_start_time
                logger.info(f"\n[Batch {batch_count}][Asset {idx}] [DONE] Completed asset processing in {asset_time:.2f}s")
                logger.info("Summary of nested items:")
                for field in nested_fields:
                    logger.info(f"  - {field}: {len(complete_asset[field])} items")

            if len(assets) < limit:
                logger.info(f"\n[Batch {batch_count}] [DONE] Retrieved fewer assets ({len(assets)}) than limit ({limit})")
                logger.info("Ending pagination")
                break
                
            paginate = assets[-1]['id']
            batch_time = time.time() - batch_start_time
            logger.info(f"\n[Batch {batch_count}] [DONE] Completed batch in {batch_time:.2f}s")
            logger.info(f"Processed {len(assets)} assets in this batch")
            logger.info(f"Total assets processed so far: {len(all_assets)}")
        else:
            logger.warning(f"[Batch {batch_count}] [WARNING] Unexpected response structure")
            # Log the problematic response
            logger.warning(f"Response structure: {object_response.keys() if object_response else None}")
            break

    total_time = time.time() - start_time
    logger.info("\n" + "="*60)
    logger.info(f"[DONE] Completed processing {asset_type_name}")
    logger.info(f"Total assets processed: {len(all_assets)}")
    logger.info(f"Total batches processed: {batch_count}")
    logger.info(f"Total time taken: {total_time:.2f} seconds")
    logger.info(f"Average time per asset: {total_time/len(all_assets):.2f} seconds")
    logger.info("="*60)
    
    return all_assets

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

    # Update flattened with relation names and their IDs
    for rel_type, values in relation_types.items():
        flattened[rel_type] = ', '.join(values)
        flattened[f"{rel_type} Asset IDs"] = ', '.join(str(id) for id in relation_ids[rel_type])

    return flattened

def save_data(data, file_name, format='excel'):
    logger.info(f"Starting to save data with format: {format}")
    start_time = time.time()

    try:
        # Remove any invalid filename characters
        file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        full_file_path = os.path.join(file_path_to_save, file_name)
        
        df = pd.DataFrame(data)
        logger.debug(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")

        if format == 'json':
            json_file = f'{full_file_path}.json'
            df.to_json(json_file, orient='records', indent=2)
            output_file = json_file
        elif format == 'csv':
            csv_file = f'{full_file_path}.csv'
            df.to_csv(csv_file, index=False)
            output_file = csv_file
        else:
            excel_file = f'{full_file_path}.xlsx'
            df.to_excel(excel_file, index=False)
            output_file = excel_file

        end_time = time.time()
        logger.info(f"Successfully saved data to {output_file} in {end_time - start_time:.2f} seconds")
        return output_file

    except Exception as e:
        logger.exception(f"Failed to save data: {str(e)}")
        raise

def process_asset_type(asset_type_id):
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(asset_type_id)

    if all_assets:
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        output_filename = f"{asset_type_name}"
        output_file = save_data(flattened_assets, output_filename, OUTPUT_FORMAT)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f"Time taken to process {asset_type_name}: {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logger.critical(f"No data to save")
        return 0

def main():
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
                    logger.info(f"Successfully processed asset type ID: {asset_type_id}")
                else:
                    failed_exports += 1
                    logger.error(f"Failed to process asset type ID: {asset_type_id}")
            except Exception as e:
                failed_exports += 1
                logger.exception(f"Error processing asset type ID {asset_type_id}: {str(e)}")
    
    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    
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