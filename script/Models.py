from configs import *
from DataMocking import DataMocking


class Models(DataMocking):
    def model_fields(self, **args):
        if self.get_table_data_lenght("measurement_units", limit=7) < 6:
            self.insert_measurement_units()
            print("Measurement units table inserted successfully")
        else:
            print("Measurement units table had been inserted before this run.")
        if self.get_table_data_lenght("fields", limit=1) > 0:
            print("Fields units table had been inserted before this run.")
        else:
            self.insert_fields()
            print("Fields units table inserted successfully")

    def model_product_types(self, **args):
        if self.get_table_data_lenght("product_types", limit=1) > 0:
            print("Product types units table had been inserted before this run.")
        else:
            self.insert_product_types()
            print("Product types units table inserted successfully")

    def model_products(self, **args):
        if self.get_table_data_lenght("products", limit=1) > 0:
            print("Products table had been inserted before this run.")
        else:
            self.insert_products()
            print("Products table inserted successfully")

    def model_users(self, **args):
        if self.get_table_data_lenght("users", limit=6) > 5:
            print("Users had been inserted before this run.")
        else:
            self.insert_users(
                args.get("min_users_per_community", min_users_per_community1),
                args.get("max_users_per_community", max_users_per_community1),
            )
            print("Users table inserted successfully")

    def model_records(self, **args):
        if self.get_table_data_lenght("records", limit=1) > 0:
            print("Records table had been inserted before this run.")
        else:
            self.insert_records()
            print("Records table inserted successfully")

    def model_portable_devices(self, **args):
        if self.get_table_data_lenght("portable_devices", limit=1) > 0:
            print("Portable devices table had been inserted before this run.")
        else:
            self.insert_portable_devices()
            print("Portable devices table inserted successfully")

    def model_portable_devices_communities(self, **args):
        if self.get_table_data_lenght("portable_devices_communities", limit=1) > 0:
            print(
                "Portable devices communities table had been inserted before this run."
            )
        else:
            self.insert_portable_devices_communities()
            print("Portable devices communities table inserted successfully")

    def model_plantings(self, **args):
        if self.get_table_data_lenght("plantings", limit=1) > 0:
            print("Plantings table had been inserted before this run.")
        else:
            self.insert_plantings()
            print("Plantings table inserted successfully")

    def model_planting_devices(self, **args):
        if self.get_table_data_lenght("planting_devices", limit=1) > 0:
            print("Planting devices table had been inserted before this run.")
        else:
            self.insert_planting_devices()
            print("Planting devices table inserted successfully")
