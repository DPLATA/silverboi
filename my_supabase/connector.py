import polars as pl
import pandas as pd
from supabase import create_client, Client


class SupabaseConnector:
    def __init__(self, supabase_url, supabase_key):
        self.supabase: Client = create_client(supabase_url, supabase_key)

    def fetch_data_into_polars(self, table_name):
        # Fetch data from Supabase into a Polars DataFrame
        response = self.supabase.table(table_name).select("*").execute()
        data = response.data
        polar_df = pl.DataFrame(data)
        return polar_df

    def fetch_data_into_pandas(self, table_name):
        # Fetch data from Supabase into a Pandas DataFrame
        response = self.supabase.table(table_name).select("*").execute()
        data = response.data
        pandas_df = pd.DataFrame(data)
        return pandas_df