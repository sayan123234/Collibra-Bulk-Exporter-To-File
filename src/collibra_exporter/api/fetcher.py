"""
Data Fetcher Module

This module provides functionality for fetching data from the Collibra API.
"""

import json
import time
import logging
import requests
from . import get_auth_header
from . import get_query, get_nested_query

logger = logging.getLogger(__name__)

def make_request(url, method='post', **kwargs):
    """
    Make a request with automatic token refresh handling.
    
    Args:
        url: The URL to make the request to
        method: The HTTP method to use (default: 'post')
        **kwargs: Additional arguments to pass to the request
        
    Returns:
        requests.Response: The response from the server
        
    Raises:
        requests.RequestException: If the request fails
    """
    try:
        # Always get fresh headers before making a request
        headers = get_auth_header()
        if 'headers' in kwargs:
            kwargs['headers'].update(headers)
        else:
            kwargs['headers'] = headers

        session = requests.Session()
        response = getattr(session, method)(url=url, **kwargs)
        response.raise_for_status()
        return response
    except requests.RequestException as error:
        logger.error(f"Request failed: {str(error)}")
        raise

def fetch_data(base_url, asset_type_id, paginate, limit, nested_offset=0, nested_limit=50):
    """
    Fetch initial data batch with basic nested limits.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: The ID of the asset type to fetch
        paginate: Pagination token or None for first page
        limit: Maximum number of assets to fetch
        nested_offset: Offset for nested fields
        nested_limit: Limit for nested fields
        
    Returns:
        dict: The response data, or None if the request fails
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

def fetch_nested_data(base_url, asset_type_id, asset_id, field_name, nested_limit=20000):
    """
    Fetch all nested data for a field, using pagination if necessary.
    
    Args:
        base_url: The base URL of the Collibra instance
        asset_type_id: ID of the asset type
        asset_id: ID of the specific asset
        field_name: Name of the nested field to fetch
        nested_limit: Initial limit for number of nested items
    
    Returns:
        list: List of all nested items for the field or None if an error occurs
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
