import os
from numpy import sort
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()

MONGODB_CONNECT_URL = os.getenv("MONGODB_CONNECT_URL")

client = MongoClient(MONGODB_CONNECT_URL)

db = client.thestackreport
