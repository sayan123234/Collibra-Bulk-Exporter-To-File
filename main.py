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
    """Configure logging with both file and console handlers, saving logs with timestamps."""
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Create timestamp for log filename
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    log_filename = f'collibra_exporter_{timestamp}.log'
    log_file = os.path.join(log_dir, log_filename)

    # Create formatters and handlers
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s'
    )

    # File handler with rotation
    # Keep 10MB per file, with 10 backup files
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=10          # Keep 10 backup files
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
    
    # Remove any existing handlers
    logger.handlers = []
    
    # Add our handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Log the start of a new session
    logger.info("="*60)
    logger.info(f"Starting new logging session at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file created at: {log_file}")
    logger.info("="*60)

    # Create a cleanup function for old logs
    def cleanup_old_logs(log_dir, max_days=30):
        """Remove log files older than max_days."""
        current_time = time.time()
        logger.info(f"Checking for logs older than {max_days} days")
        
        for filename in os.listdir(log_dir):
            if filename.endswith('.log'):
                filepath = os.path.join(log_dir, filename)
                file_time = os.path.getmtime(filepath)
                
                if (current_time - file_time) > (max_days * 24 * 60 * 60):
                    try:
                        os.remove(filepath)
                        logger.info(f"Removed old log file: {filename}")
                    except Exception as e:
                        logger.warning(f"Could not remove old log file {filename}: {str(e)}")

    # Run cleanup for logs older than 30 days
    cleanup_old_logs(log_dir)

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
        
        # Get initial batch of assets
        object_response = fetch_data(asset_type_id, paginate, limit, 0, nested_limit)
        if not object_response or 'data' not in object_response or 'assets' not in object_response['data']:
            logger.error(f"[Batch {batch_count}] Failed to fetch initial data")
            break

        current_assets = object_response['data']['assets']
        if not current_assets:
            logger.info(f"[Batch {batch_count}] No more assets to fetch")
            break

        logger.info(f"[Batch {batch_count}] Processing {len(current_assets)} assets")

        # Process each asset
        processed_assets = []
        for asset_idx, asset in enumerate(current_assets, 1):
            asset_id = asset['id']
            asset_name = asset.get('displayName', 'Unknown Name')
            logger.info(f"\n[Batch {batch_count}][Asset {asset_idx}/{len(current_assets)}] Processing: {asset_name}")
            
            # Initialize complete asset with base data
            complete_asset = asset.copy()
            
            # Define nested fields to process
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

            # Process each nested field
            for field in nested_fields:
                field_data = []
                if field in asset:
                    field_data.extend(asset[field])
                    
                    # Continue fetching if initial data hits the limit
                    if len(asset[field]) == nested_limit:
                        offset = nested_limit
                        while True:
                            logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                      f"Fetching more data from offset {offset}...")
                            
                            nested_response = fetch_data(asset_type_id, paginate, limit, offset, nested_limit)
                            
                            if not nested_response or 'data' not in nested_response or \
                               'assets' not in nested_response['data'] or \
                               not nested_response['data']['assets']:
                                break

                            # Find the corresponding asset in the response
                            matching_asset = None
                            for resp_asset in nested_response['data']['assets']:
                                if resp_asset['id'] == asset_id:
                                    matching_asset = resp_asset
                                    break

                            if not matching_asset or field not in matching_asset or \
                               not matching_asset[field]:
                                break

                            additional_data = matching_asset[field]
                            field_data.extend(additional_data)
                            
                            logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                      f"Retrieved {len(additional_data)} more items. "
                                      f"Total so far: {len(field_data)}")

                            if len(additional_data) < nested_limit:
                                break
                                
                            offset += nested_limit

                logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                          f"Final count: {len(field_data)} items")
                complete_asset[field] = field_data

            processed_assets.append(complete_asset)
            logger.info(f"[Batch {batch_count}][Asset {asset_idx}] Completed processing")

        all_assets.extend(processed_assets)
        
        if len(current_assets) < limit:
            logger.info(f"[Batch {batch_count}] Retrieved fewer assets than limit, ending pagination")
            break
            
        paginate = current_assets[-1]['id']
        batch_time = time.time() - batch_start_time
        logger.info(f"\n[Batch {batch_count}] Completed batch in {batch_time:.2f}s")
        logger.info(f"Total assets processed so far: {len(all_assets)}")

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