"""
Data Exporter Module

This module provides functionality for exporting Collibra data to various formats.
"""

import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def save_data(data, file_name, format='excel', output_dir='outputs'):
    """
    Save data to a file in the specified format.
    
    Args:
        data: The data to save (list of dictionaries)
        file_name: The name of the file (without extension)
        format: The format to save the data in ('json', 'csv', or 'excel')
        output_dir: The directory to save the file in
        
    Returns:
        str: The path to the saved file
    
    Raises:
        Exception: If there is an error saving the data
    """
    logger.info(f"Starting to save data with format: {format}")
    start_time = pd.Timestamp.now()

    try:
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Remove any invalid filename characters
        file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        full_file_path = os.path.join(output_dir, file_name)
        
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
        else:  # default to excel
            excel_file = f'{full_file_path}.xlsx'
            df.to_excel(excel_file, index=False)
            output_file = excel_file

        end_time = pd.Timestamp.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Successfully saved data to {output_file} in {duration:.2f} seconds")
        return output_file

    except Exception as e:
        logger.exception(f"Failed to save data: {str(e)}")
        raise
