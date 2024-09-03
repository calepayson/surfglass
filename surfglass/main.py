from dotenv import load_dotenv
import os

def get_api_key():
    load_dotenv()
    return os.getenv('API_KEY')
