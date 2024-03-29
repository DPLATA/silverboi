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

# # Select only the desired columns and create a new DataFrame
# users_selected_columns = ['id', 'age', 'name', 'email', 'title', 'gender', 'height', 'weight', 'address', 'language', 'telephone', 'blood_type', 'created_at', 'occupation', 'updated_at', 'nationality', 'academic_degree']
# products_selected_columns = ['id', 'make', 'year', 'model', 'price', 'created_at', 'updated_at']
# purchases_selected_columns = ['id', 'user_id', 'created_at', 'product_id', 'updated_at', 'returned_at', 'purchased_at', 'added_to_cart_at']
#
# users = users[users_selected_columns]
# products = products[products_selected_columns]
# purchases = purchases[purchases_selected_columns]
#
# fe = FeatureEngine(users, products, purchases)
# u, pr, pu, upi = fe.perform_feature_engineering()
#
# print('')

manager = FeatureStoreManager(hopsworks_api_key_value)

users_fg = manager.get_or_create_feature_group(
    feature_group_name="users",
    version=1,
    description="users data",
    primary_key=["id"]
)

write_options = {"wait_for_job": True}

manager.insert_data_into_feature_group(feature_group=users_fg, data_frame=users, write_options=write_options)

# users_fg.insert(
#     users,
#     write_options={"wait_for_job": True},
# )

feature_descriptions = [
    {"name": "id", "description": "user id"},
    {"name": "age", "description": "age"},
    {"name": "name", "description": "name"},
    {"name": "email", "description": "contact email"},
    {"name": "title", "description": "title"},
    {"name": "gender", "description": "gender"},
    {"name": "height", "description": "height"},
    {"name": "weight", "description": "weight"},
    {"name": "address", "description": "contact address"},
    {"name": "language", "description": "primary contact language"},
    {"name": "telephone", "description": "primary contact telephone"},
    {"name": "blood_type", "description": "contact blood type"},
    {"name": "created_at", "description": "created at date"},
    {"name": "occupation", "description": "occupation"},
    {"name": "updated_at", "description": "updated at date"},
    {"name": "nationality", "description": "nationality"},
    {"name": "academic_degree", "description": "academic degree"},
    {"name": "average_age", "description": "nationality"},
]

for desc in feature_descriptions:
    users_fg.update_feature_description(desc["name"], desc["description"])

