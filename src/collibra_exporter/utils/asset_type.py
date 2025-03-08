"""
Asset Type Utilities

This module provides functions for working with Collibra asset types.
"""

import os
import logging
import requests
from dotenv import load_dotenv
from functools import lru_cache
from ..api import get_auth_header

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

session = requests.Session()

@lru_cache(maxsize=100)
def get_asset_type_name(asset_type_id):
    """
    Get the name of an asset type by its ID.
    
    Args:
        asset_type_id: The ID of the asset type
        
    Returns:
        str: The name of the asset type, or None if not found
    """
    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}/rest/2.0/assetTypes/{asset_type_id}"

    try:
        session.headers.update(get_auth_header())
        response = session.get(url)
        response.raise_for_status()
        json_response = response.json()
        return json_response["name"]
    except requests.RequestException as e:
        logging.error(f"Asset type not found in Collibra: {e}")
        return None

@lru_cache(maxsize=1)
def get_available_asset_type():
    """
    Get all available asset types from Collibra.
    
    Returns:
        dict: A dictionary containing a list of asset types with their IDs and names,
              or None if the request fails
    """
    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}/rest/2.0/assetTypes"

    try:
        session.headers.update(get_auth_header())
        response = session.get(url)
        response.raise_for_status()
        original_results = response.json()["results"]
        modified_results = [{"id": asset["id"], "name": asset["name"]} for asset in original_results]
        
        logger.info(f"Successfully retrieved {len(modified_results)} asset types")
        return {"results": modified_results}
    except requests.RequestException as e:
        logger.error(f"Failed to retrieve asset types: {e}")
        return None
