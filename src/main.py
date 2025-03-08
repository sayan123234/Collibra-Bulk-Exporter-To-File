#!/usr/bin/env python3
"""
Collibra Bulk Exporter

This script exports assets from Collibra based on asset type IDs.
"""

import os
import json
import sys
from dotenv import load_dotenv
from collibra_exporter import (
    setup_logging,
    process_all_asset_types
)

def main():
    """Main entry point for the Collibra Bulk Exporter."""
    # Setup logging
    logger = setup_logging()
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Get configuration from environment
        base_url = os.getenv('COLLIBRA_INSTANCE_URL')
        if not base_url:
            logger.error("COLLIBRA_INSTANCE_URL environment variable is not set")
            sys.exit(1)
            
        output_dir = os.getenv('FILE_SAVE_LOCATION', 'outputs')
        output_format = os.getenv('OUTPUT_FORMAT', 'csv').lower()
        
        # Validate output format
        if output_format not in ['csv', 'json', 'excel']:
            logger.warning(f"Invalid output format: {output_format}. Defaulting to CSV.")
            output_format = 'csv'
        
        # Load asset type IDs from configuration file
        config_path = os.getenv('CONFIG_PATH', 'config/Collibra_Asset_Type_Id_Manager.json')
        try:
            with open(config_path, 'r') as file:
                config = json.load(file)
                asset_type_ids = config.get('ids', [])
                
            if not asset_type_ids:
                logger.error("No asset type IDs found in configuration file")
                sys.exit(1)
                
            logger.info(f"Loaded {len(asset_type_ids)} asset type IDs from {config_path}")
                
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading configuration file: {str(e)}")
            sys.exit(1)
        
        # Process all asset types
        successful, failed, total_time = process_all_asset_types(
            base_url,
            asset_type_ids,
            output_format,
            output_dir
        )
        
        # Log summary
        logger.info("\nExport completed!")
        logger.info(f"Successful exports: {successful}")
        logger.info(f"Failed exports: {failed}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        # Return exit code based on success/failure
        if failed > 0:
            return 1
        return 0
        
    except Exception as e:
        logger.exception("Fatal error in main program")
        return 1

if __name__ == "__main__":
    sys.exit(main())
