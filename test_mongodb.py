
from pymongo import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://adarsh:vesitupdate@eqstocksdb.uojhq.mongodb.net/?retryWrites=true&w=majority&appName=EqStocksDb"


# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

database = client.get_database('sample_mflix')  
collection_movies = database.get_collection('movies')  
print(collection_movies)