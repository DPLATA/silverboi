import pandas as pd


class FeatureEngine:
    def __init__(self, users, products, purchases):
        self.users = users
        self.products = products
        self.purchases = purchases

    def perform_feature_engineering(self):
        # 1. User-specific features
        self.users['average_age'] = self.users['age'].mean()
        self.users = pd.get_dummies(self.users, columns=['gender'], prefix=['gender'])

        # 2. Product-specific features
        self.products = pd.get_dummies(self.products, columns=['make', 'model'], prefix=['make', 'model'])

        # 3. Purchase history features
        self.purchases['purchased_at'] = pd.to_datetime(self.purchases['purchased_at'])
        self.purchases.sort_values(by=['user_id', 'purchased_at'], inplace=True)

        # Calculate the time elapsed since the user's last purchase
        self.purchases['time_since_last_purchase'] = self.purchases.groupby('user_id')['purchased_at'].diff().dt.days

        # 4. Interaction features
        user_product_interaction = pd.merge(self.users, self.purchases, left_on='id', right_on='user_id', how='left')
        user_product_interaction = pd.merge(user_product_interaction, self.products, left_on='product_id', right_on='id', how='left')
        user_product_interaction['avg_age_and_price'] = user_product_interaction['average_age'] * user_product_interaction['price']

        # 5. Temporal features
        self.purchases['purchase_month'] = self.purchases['purchased_at'].dt.month

        return self.users, self.products, self.purchases, user_product_interaction
