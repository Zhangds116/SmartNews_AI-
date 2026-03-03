# fetch_news.py

import requests
from config import API_URL

def fetch_news():
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    data = response.json()

    if data.get("status") != "200":
        raise ValueError("API返回异常")

    return data.get("data", [])