import requests
import os
import logging
from dotenv import load_dotenv
from functools import lru_cache

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

session = requests.Session()

@lru_cache(maxsize=1)
def oauth_bearer_token():
    
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}.com/rest/oauth/v2/token"

    payload = f'client_id={client_id}&grant_type=client_credentials&client_secret={client_secret}'

    try:
        response = session.post(url=url, data=payload)
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.RequestException as e:
        logging.error(f"Error obtaining OAuth token: {e}")