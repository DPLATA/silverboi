import os
from dotenv import load_dotenv
from my_supabase.connector import SupabaseConnector
from helpers.feature_engine import FeatureEngine
from my_hopsworks.connector import FeatureStoreManager
import polars as pl

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
supabase_key: str = os.environ.get("SUPABASE_KEY")
hopsworks_api_key_value: str = os.environ.get("HOPSWORKS_API_KEY_VALUE")
USERS = 'users'
PRODUCTS = 'products'
PURCHASES = 'purchases'

connector = SupabaseConnector(url, supabase_key)

# Fetch data into Polars DataFrame
users = connector.fetch_data_into_pandas(USERS)
products = connector.fetch_data_into_pandas(PRODUCTS)
purchases = connector.fetch_data_into_pandas(PURCHASES)

# Select only the desired columns and create a new DataFrame
users_selected_columns = ['id', 'age', 'name', 'email', 'title', 'gender', 'height', 'weight', 'address', 'language', 'telephone', 'blood_type', 'occupation', 'nationality']
products_selected_columns = ['id', 'make', 'year', 'model', 'price']
purchases_selected_columns = ['id', 'user_id', 'product_id', 'returned_at', 'purchased_at', 'added_to_cart_at']

users = users[users_selected_columns]
products = products[products_selected_columns]
purchases = purchases[purchases_selected_columns]

fe = FeatureEngine(users, products, purchases)
u, pr, pu, upi = fe.perform_feature_engineering()

print('')

# manager = FeatureStoreManager(hopsworks_api_key_value)
#
# users_fg = manager.fs.get_or_create_feature_group(
#     name="users",
#     version=1,
#     description="Users data",
#     primary_key=["id"]
# )
#
# users_fg.insert(
#     users,
#     write_options={"wait_for_job": True},
# )
#
# feature_descriptions = [
#     {"name": "id", "description": "user id"},
#     {"name": "age", "description": "age"},
#     {"name": "name", "description": "name"},
#     {"name": "email", "description": "contact email"},
#     {"name": "title", "description": "title"},
#     {"name": "height", "description": "height"},
#     {"name": "weight", "description": "weight"},
#     {"name": "address", "description": "contact address"},
#     {"name": "language", "description": "primary contact language"},
#     {"name": "telephone", "description": "primary contact telephone"},
#     {"name": "blood_type", "description": "contact blood type"},
#     {"name": "occupation", "description": "occupation"},
#     {"name": "nationality", "description": "nationality"},
# ]
#
# for desc in feature_descriptions:
#     users_fg.update_feature_description(desc["name"], desc["description"])

