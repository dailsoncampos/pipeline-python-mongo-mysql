from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import os
from dotenv import load_dotenv

def connect_mongo(uri):
    # Criando um novo cliente e conectando ao servidor
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Enviando um ping pra conferir se a conexão foi realizada
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    return db[col_name]

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    result = col.insert_many(data)
    n_docs_inseridos = len(result.inserted_ids)
    return n_docs_inseridos 


if __name__ == "__main__":
    
    load_dotenv()

    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    cluster = os.getenv("MONGO_CLUSTER")
    app_name = os.getenv("MONGO_APP_NAME")

    uri = f"mongodb+srv://{user}:{password}@{cluster}.iwo5r.mongodb.net/?retryWrites=true&w=majority&appName={app_name}"

    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos")
    col = create_connect_collection(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")

    client.close()