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
from graphql_query import get_query, get_nested_query
from get_asset_type_name import get_asset_type_name
from OauthAuth import get_auth_header
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

session = requests.Session()

def make_request(url, method='post', **kwargs):
    """Make a request with automatic token refresh handling."""
    try:
        # Always get fresh headers before making a request
        headers = get_auth_header()
        if 'headers' in kwargs:
            kwargs['headers'].update(headers)
        else:
            kwargs['headers'] = headers

        response = getattr(session, method)(url=url, **kwargs)
        response.raise_for_status()
        return response
    except requests.RequestException as error:
        logger.error(f"Request failed: {str(error)}")
        raise

def fetch_data(asset_type_id, paginate, limit, nested_offset=0, nested_limit=50):
    """
    Fetch initial data batch with basic nested limits.
    """
    try:
        query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null', nested_offset, nested_limit)
        variables = {'limit': limit}
        logger.debug(f"Sending GraphQL request for asset_type_id: {asset_type_id}, paginate: {paginate}, nested_offset: {nested_offset}")

        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        
        response = make_request(
            url=graphql_url,
            json={
                'query': query,
                'variables': variables
            }
        )
        
        response_time = time.time() - start_time
        logger.debug(f"GraphQL request completed in {response_time:.2f} seconds")

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

def fetch_nested_data(asset_type_id, asset_id, field_name, nested_limit=20000):
    """
    Fetch all nested data for a field, using pagination if necessary.
    
    Args:
        asset_type_id: ID of the asset type
        asset_id: ID of the specific asset
        field_name: Name of the nested field to fetch
        nested_limit: Initial limit for number of nested items
    
    Returns:
        List of all nested items for the field or None if an error occurs
    """
    try:
        # First attempt with maximum limit to see if pagination is needed
        query = get_nested_query(asset_type_id, asset_id, field_name, 0, nested_limit)
        
        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        
        response = make_request(
            url=graphql_url,
            json={'query': query}
        )
        
        response_time = time.time() - start_time
        logger.debug(f"Nested GraphQL request completed in {response_time:.2f} seconds")

        data = response.json()
        if 'errors' in data:
            logger.error(f"GraphQL errors in nested query: {data['errors']}")
            return None
            
        if not data['data']['assets']:
            logger.error(f"No asset found in nested query response")
            return None
            
        initial_results = data['data']['assets'][0][field_name]
        
        # If we hit the limit, use pagination to fetch all results
        if len(initial_results) == nested_limit:
            logger.info(f"Hit nested limit of {nested_limit} for {field_name}, switching to pagination")
            
            # Use pagination to fetch all items
            all_items = []
            offset = 0
            batch_number = 1
            batch_size = nested_limit

            # Add initial results to our collection
            all_items.extend(initial_results)
            offset += nested_limit
            
            # Continue fetching batches until we get fewer items than requested
            while True:
                logger.info(f"Fetching batch {batch_number} for {field_name} (offset: {offset})")
                
                query = get_nested_query(asset_type_id, asset_id, field_name, offset, batch_size)
                
                try:
                    response = make_request(
                        url=f"https://{base_url}/graphql/knowledgeGraph/v1",
                        json={'query': query}
                    )
                    
                    data = response.json()
                    
                    if 'errors' in data:
                        logger.error(f"GraphQL errors in nested query: {data['errors']}")
                        break
                        
                    if not data['data']['assets']:
                        logger.error(f"No asset found in nested query response")
                        break
                        
                    current_items = data['data']['assets'][0][field_name]
                    current_batch_size = len(current_items)
                    
                    all_items.extend(current_items)
                    logger.info(f"Retrieved {current_batch_size} items in batch {batch_number}")
                    
                    # If we got fewer items than the batch size, we've reached the end
                    if current_batch_size < batch_size:
                        break
                        
                    offset += batch_size
                    batch_number += 1
                    
                except Exception as e:
                    logger.exception(f"Failed to fetch batch {batch_number} for {field_name}: {str(e)}")
                    break

            logger.info(f"Completed fetching {field_name}. Total items: {len(all_items)}")
            return all_items
            
        # If we didn't hit the limit, return the initial results
        return initial_results
    except Exception as e:
        logger.exception(f"Failed to fetch nested data for {field_name}: {str(e)}")
        return None

def process_data(asset_type_id, limit=94, initial_nested_limit=50):
    """
    Process assets with optimized nested field handling.
    """
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info("="*60)
    logger.info(f"Starting data processing for asset type: {asset_type_name} (ID: {asset_type_id})")
    logger.info(f"Configuration - Batch Size: {limit}, Initial Nested Limit: {initial_nested_limit}")
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
        
        # Get initial batch with small nested limits
        initial_response = fetch_data(
            asset_type_id, 
            paginate, 
            limit, 
            0, 
            initial_nested_limit
        )
        
        if not initial_response or 'data' not in initial_response or 'assets' not in initial_response['data']:
            logger.error(f"[Batch {batch_count}] Failed to fetch initial data")
            break

        current_assets = initial_response['data']['assets']
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
            
            complete_asset = asset.copy()
            
            # Define nested fields to check
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

            # Check each nested field
            for field in nested_fields:
                if field not in asset:
                    continue
                    
                initial_data = asset[field]
                
                # If we hit the initial limit, fetch all data in one big query
                if len(initial_data) == initial_nested_limit:
                    logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] Requires full fetch")
                    
                    complete_data = fetch_nested_data(
                        asset_type_id,
                        asset_id,
                        field
                    )
                    
                    if complete_data:
                        complete_asset[field] = complete_data
                        logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                  f"Retrieved {len(complete_data)} items")
                    else:
                        logger.warning(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                     f"Failed to fetch complete data, using initial data")
                        complete_asset[field] = initial_data
                else:
                    complete_asset[field] = initial_data

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
    avg_time = total_time/len(all_assets) if all_assets else 0
    logger.info(f"Average time per asset: {avg_time:.2f} seconds")
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
    """
    Process a single asset type by ID.
    
    This function:
    1. Gets the asset type name
    2. Processes all assets of this type using process_data
    3. Flattens the JSON structure for each asset
    4. Saves the data to a file in the specified format
    
    Args:
        asset_type_id: The ID of the asset type to process
        
    Returns:
        float: The time taken to process the asset type in seconds, or 0 if no data was processed
    """
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
