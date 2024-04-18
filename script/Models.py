from configs import *


class Models:
    def __init__(self, data_mocking):
        self.data_mocking = data_mocking

    def model_fields(self, **args):
        if self.data_mocking.get_table_data_lenght("measurement_units", limit=7) < 6:
            self.data_mocking.insert_measurement_units()
            print("Measurement units table inserted successfully")
        else:
            print("Measurement units table had been inserted before this run.")
        if self.data_mocking.get_table_data_lenght("fields", limit=1) > 0:
            print("Fields units table had been inserted before this run.")
        else:
            self.data_mocking.insert_fields()
            print("Fields units table inserted successfully")

    def model_product_types(self, **args):
        if self.data_mocking.get_table_data_lenght("product_types", limit=1) > 0:
            print("Product types units table had been inserted before this run.")
        else:
            self.data_mocking.insert_product_types()
            print("Product types units table inserted successfully")

    def model_products(self, **args):
        if self.data_mocking.get_table_data_lenght("products", limit=1) > 0:
            print("Products table had been inserted before this run.")
        else:
            self.data_mocking.insert_products()
            print("Products table inserted successfully")

    def model_users(self, **args):
        if self.data_mocking.get_table_data_lenght("users", limit=6) > 5:
            print("Users had been inserted before this run.")
        else:
            self.data_mocking.insert_users(
                args.get("min_users_per_community", min_users_per_community),
                args.get("max_users_per_community", max_users_per_community),
                args.get("community_weights", community_weights)
            )
            print("Users table inserted successfully")

    def model_records(self, **args):
        if self.data_mocking.get_table_data_lenght("records", limit=1) > 0:
            print("Records table had been inserted before this run.")
        else:
            self.data_mocking.insert_records(
                args.get("duration", duration)
            )
            print("Records table inserted successfully")

    def model_portable_devices(self, **args):
        if self.data_mocking.get_table_data_lenght("portable_devices", limit=1) > 0:
            print("Portable devices table had been inserted before this run.")
        else:
            self.data_mocking.insert_portable_devices()
            print("Portable devices table inserted successfully")

    def model_portable_devices_communities(self, **args):
        if (
            self.data_mocking.get_table_data_lenght(
                "portable_devices_communities", limit=1
            )
            > 0
        ):
            print(
                "Portable devices communities table had been inserted before this run."
            )
        else:
            self.data_mocking.insert_portable_devices_communities()
            print("Portable devices communities table inserted successfully")

    def model_plantings(self, **args):
        if self.data_mocking.get_table_data_lenght("plantings", limit=1) > 0:
            print("Plantings table had been inserted before this run.")
        else:
            self.data_mocking.insert_plantings(
                args.get("duration", duration)
            )
            print("Plantings table inserted successfully")

    def model_planting_devices(self, **args):
        if self.data_mocking.get_table_data_lenght("planting_devices", limit=1) > 0:
            print("Planting devices table had been inserted before this run.")
        else:
            self.data_mocking.insert_planting_devices()
            print("Planting devices table inserted successfully")

    def model_harvests(self, **args):
        if self.data_mocking.get_table_data_lenght("harvests", limit=1) > 0:
            print("Harvests table had been inserted before this run.")
        else:
            self.data_mocking.insert_harvests()
            print("Harvests table inserted successfully")

    def model_measurement_units(self, **args):
        if self.data_mocking.get_table_data_lenght("measurement_units", limit=6) > 5:
            print("Measurement units table had been inserted before this run.")
        else:
            self.data_mocking.insert_measurement_units()
            print("Measurement units table inserted successfully")

    def model_harvest_devices(self, **args):
        if self.data_mocking.get_table_data_lenght("harvest_devices", limit=1) > 0:
            print("Harvest devices table had been inserted before this run.")
        else:
            self.data_mocking.insert_harvest_devices()
            print("Harvest devices table inserted successfully")

    def model_precipitation_types(self, **args):
        if self.data_mocking.get_table_data_lenght("prec_types", limit=2) > 1:
            print("Precipitation types table had been inserted before this run.")
        else:
            self.data_mocking.insert_precipitation_types()
            print("Precipitation types table inserted successfully")

    def model_weather_metrics(self, **args):
        if self.data_mocking.get_table_data_lenght("weather_metrics", limit=1) > 0:
            print("Weather metrics table had been inserted before this run.")
        else:
            self.data_mocking.insert_weather_metrics()
            print("Weather metrics table inserted successfully")

    def model_cultivations(self, **args):
        if self.data_mocking.get_table_data_lenght("cultivations", limit=1) > 0:
            print("Cultivation table had been inserted before this run.")
        else:
            self.data_mocking.insert_cultivations()
            print("Cultivation table inserted successfully")

    def model_cultivation_devices(self, **args):
        if self.data_mocking.get_table_data_lenght("cultivation_devices", limit=1) > 0:
            print("Cultivation devices table had been inserted before this run.")
        else:
            self.data_mocking.insert_cultivation_devices()
            print("Cultivation devices table inserted successfully")

    def model_devices_calendars(self, **args):
        if self.data_mocking.get_table_data_lenght("devices_calendars", limit=1) > 0:
            print("Devices calendars table had been inserted before this run.")
        else:
            self.data_mocking.insert_devices_calendars()
            print("Devices calendars table inserted successfully")
