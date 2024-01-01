import hopsworks


class FeatureStoreManager:
    def __init__(self, api_key_value):
        self.project = hopsworks.login(api_key_value=api_key_value)
        self.fs = self.project.get_feature_store()

    def get_feature_group(self, feature_group_name, version=1):
        return self.fs.get_external_feature_group(feature_group_name, version=version)

    def insert_data_into_feature_group(self, feature_group, data_frame, write_options=None):
        feature_group.insert(data_frame, write_options=write_options)
        print('Insert Done')





