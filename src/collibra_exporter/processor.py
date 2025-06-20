"""
Data Processor Module

This module provides the core functionality for processing Collibra assets.
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .api import fetch_data, fetch_nested_data
from .utils import get_asset_type_name
from .models import flatten_json, save_data

logger = logging.getLogger(__name__)

def process_data(base_url, asset_type_id, limit=94, initial_nested_limit=50):
    """
    Process assets with optimized nested field handling.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to process
        limit: Maximum number of assets to fetch per batch
        initial_nested_limit: Initial limit for nested fields
        
    Returns:
        list: A list of processed assets
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
            base_url,
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
                        base_url,
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

def process_asset_type(base_url, asset_type_id, output_format, output_dir):
    """
    Process a single asset type by ID.
    
    This function:
    1. Gets the asset type name
    2. Processes all assets of this type using process_data
    3. Flattens the JSON structure for each asset
    4. Saves the data to a file in the specified format
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to process
        output_format: The format to save the data in ('json', 'csv', or 'excel')
        output_dir: The directory to save the output files in
        
    Returns:
        float: The time taken to process the asset type in seconds, or 0 if no data was processed
    """
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(base_url, asset_type_id)

    if all_assets:
        
        #To directly save without flattening, uncomment the below commented lines
        # output_filename = f"{asset_type_name}"
        # output_file = save_data(all_assets, output_filename, output_format, output_dir)
        
        #Comment out the following three lines if flattening not required
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        output_filename = f"{asset_type_name}"
        output_file = save_data(flattened_assets, output_filename, output_format, output_dir)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f"Time taken to process {asset_type_name}: {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logger.critical(f"No data to save")
        return 0

def process_all_asset_types(base_url, asset_type_ids, output_format, output_dir, max_workers=5):
    """
    Process multiple asset types in parallel.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_ids: A list of asset type IDs to process
        output_format: The format to save the data in ('json', 'csv', or 'excel')
        output_dir: The directory to save the output files in
        max_workers: Maximum number of worker threads to use
        
    Returns:
        tuple: (successful_exports, failed_exports, total_time)
    """
    logger.info(f"Starting Collibra Bulk Exporter")
    logger.info(f"Output format: {output_format}")
    logger.info(f"Number of asset types to process: {len(asset_type_ids)}")

    total_start_time = time.time()
    successful_exports = 0
    failed_exports = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_asset = {
            executor.submit(
                process_asset_type, 
                base_url, 
                asset_type_id, 
                output_format, 
                output_dir
            ): asset_type_id for asset_type_id in asset_type_ids
        }
        
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
    logger.info(f"Total asset types processed: {len(asset_type_ids)}")
    logger.info(f"Successful exports: {successful_exports}")
    logger.info(f"Failed exports: {failed_exports}")
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    
    return successful_exports, failed_exports, total_time
