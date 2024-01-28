import os
from dotenv import load_dotenv
from my_hopsworks.connector import FeatureStoreManager
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from joblib import dump

load_dotenv()

hopsworks_api_key_value: str = os.environ.get("HOPSWORKS_API_KEY_VALUE")

manager = FeatureStoreManager(hopsworks_api_key_value)

users_fg = manager.get_feature_group("users", version=1)

query = users_fg.select_except(["id", "email", "name", "address", "telephone", "updated_at", "created_at", "average_age"])

df = query.read()

numerical_cols = ['age', 'weight']  # Add other numerical columns as necessary
categorical_cols = ['gender', 'height', 'language', 'blood_type', 'nationality', 'academic_degree']  # Add other categorical columns as necessary

# Column Transformer for preprocessing
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numerical_cols),
        ('cat', OneHotEncoder(), categorical_cols)
    ])

# Clustering pipeline
pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                           ('cluster', KMeans(n_clusters=5))])

# Fit the model
pipeline.fit(df)

# Assuming your model pipeline is named `pipeline`
model_filename = 'models/clustering_model.pkl'
dump(pipeline, model_filename)

model = manager.mr.python.create_model(
    name="clustering_model",
    description="Clustering Model"
)
model.save('models/clustering_model.pkl')