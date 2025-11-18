from motor.motor_asyncio import AsyncIOMotorClient
import os

mongo_uri = "mongodb://127.0.0.1:27017"
# mongo_uri = "mongodb://host.docker.internal:27017"  # Use this when running in Docker
MONGO_DETAILS = os.getenv("MONGO_URI", mongo_uri)

client = AsyncIOMotorClient(MONGO_DETAILS)
db = client['acclimate_db']

hbom_components = db['hbom_components']   
hbom_fragilities = db['hbom_fragilities']
hbom_definitions = db['hbom_definitions']
cost_collection = db['cost_collection']
