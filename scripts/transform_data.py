from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
from pymongo import MongoClient
import pandas as pd

def transform_data():
  # Connect to MongoDB
  mongo_client = connect_mongo()
  mongo_collection = create_connect_collection(mongo_client, 'your_mongo_db', 'your_collection')

  # Connect to MySQL
  mysql_conn = create_connect_db('your_mysql_db')

  # Fetch data from MongoDB
  mongo_data = mongo_collection.find()

  # Transform data
  transformed_data = []
  for data in mongo_data:
    transformed_record = {
      'field1': data['field1'],
      'field2': data['field2'],
      # Add your transformation logic here
    }
    transformed_data.append(transformed_record)

  # Save transformed data to MySQL
  cursor = mysql_conn.cursor()
  for record in transformed_data:
    cursor.execute(
      "INSERT INTO your_table (field1, field2) VALUES (%s, %s)",
      (record['field1'], record['field2'])
    )
  mysql_conn.commit()
  cursor.close()
  mysql_conn.close()

if __name__ == "__main__":
  transform_data()
  def visualize_collection(col):
    for doc in col.find():
      print(doc)

  def rename_column(col, col_name, new_name):
    col.update_many({}, {'$rename': {col_name: new_name}})

  def select_category(col, category):
    return list(col.find({'category': category}))

  def make_regex(col, regex):
    return list(col.find({'field': {'$regex': regex}}))

  def create_dataframe(lista):
    return pd.DataFrame(lista)

  def format_date(df):
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df

  def save_csv(df, path):
    df.to_csv(path, index=False)

  # Connect to MongoDB
  mongo_client = connect_mongo()
  mongo_collection = create_connect_collection(mongo_client, 'db_produtos_desafio', 'your_collection')

  # Visualize collection
  visualize_collection(mongo_collection)

  # Rename columns
  rename_column(mongo_collection, 'lat', 'Latitude')
  rename_column(mongo_collection, 'lon', 'Longitude')

  # Select category 'livros'
  livros_data = select_category(mongo_collection, 'livros')

  # Create DataFrame
  livros_df = create_dataframe(livros_data)

  # Save DataFrame to CSV
  save_csv(livros_df, '/path/to/livros.csv')

  # Filter products sold from 2021
  filtered_data = [doc for doc in livros_data if pd.to_datetime(doc['date']).year >= 2021]

  # Create DataFrame for filtered data
  filtered_df = create_dataframe(filtered_data)

  # Format date
  filtered_df = format_date(filtered_df)

  # Save filtered DataFrame to CSV
  save_csv(filtered_df, '/path/to/filtered_livros.csv')