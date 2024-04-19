from configs import *


class Models:
    def __init__(self, data_mocking):
        self.data_mocking = data_mocking

    def report(self, table_name, count, time):
        print(
            f"\nReport {table_name}\n inserted data count is:  {count}  \n It lasted: {time} \n"
        )

    def model_fields(self, **args):
        if self.data_mocking.get_table_data_lenght("measurement_units", limit=7) < 6:
            times, count = self.data_mocking.insert_measurement_units()
            print("\n Measurement units table inserted successfully")
            self.report("measurement_units", count, times)
        else:
            print("Measurement units table had been inserted before this run.")
        if self.data_mocking.get_table_data_lenght("fields", limit=1) > 0:
            print("Fields units table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_fields(
                args.get("min_field_count", min_field_count),
                args.get("max_field_count", max_field_count),
                args.get("min_field_size", min_field_size),
                args.get("max_field_size", max_field_size),
                args.get("field_owner_weights", field_owner_weights),
            )
            print("\nFields units table inserted successfully")
            self.report("fields", count, times)

    def model_product_types(self):
        if self.data_mocking.get_table_data_lenght("product_types", limit=3) > 2:
            print("Product types units table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_product_types()
            print("\nProduct types units table inserted successfully")
            self.report("product_types", count, times)

    def model_products(self):
        if self.data_mocking.get_table_data_lenght("products", limit=1) > 0:
            print("Products table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_products()
            print("\nProducts table inserted successfully")
            self.report("products", count, times)

    def model_users(self, **args):
        if self.data_mocking.get_table_data_lenght("users", limit=6) > 5:
            print("Users had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_users(
                args.get("min_users_per_community", min_users_per_community),
                args.get("max_users_per_community", max_users_per_community),
                args.get("community_weights", community_weights),
            )
            print("\n Users table inserted successfully \n")
            self.report("users", count, times)

    def model_records(self, **args):
        if self.data_mocking.get_table_data_lenght("records", limit=1) > 0:
            print("Records table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_records(
                args.get("duration", duration)
            )
            print("\n Records table inserted successfully")
            self.report("records", count, times)

    def model_portable_devices(self):
        if self.data_mocking.get_table_data_lenght("portable_devices", limit=1) > 0:
            print("Portable devices table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_portable_devices()
            print("\nPortable devices table inserted successfully")
            self.report("portable_devices", count, times)

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
            times, count = self.data_mocking.insert_portable_devices_communities(
                args.get("devices_weights", devices_weights)
            )
            print("\nPortable devices communities table inserted successfully")
            self.report("portable_devices_communities", count, times)

    def model_plantings(self, **args):
        if self.data_mocking.get_table_data_lenght("plantings", limit=1) > 0:
            print("Plantings table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_plantings(
                args.get("duration", duration)
            )
            print("\nPlantings table inserted successfully")
            self.report("plantings", count, times)

    def model_planting_devices(self):
        if self.data_mocking.get_table_data_lenght("planting_devices", limit=1) > 0:
            print("Planting devices table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_planting_devices()
            print("\nPlanting devices table inserted successfully")
            self.report("planting_devices", count, times)

    def model_harvests(self):
        if self.data_mocking.get_table_data_lenght("harvests", limit=1) > 0:
            print("Harvests table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_harvests()
            print("Harvests table inserted successfully")
            self.report("harvests", count, times)

    def model_measurement_units(self):
        if self.data_mocking.get_table_data_lenght("measurement_units", limit=6) > 5:
            print("Measurement units table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_measurement_units()
            print("\nMeasurement units table inserted successfully")
            self.report("measurement_units", count, times)

    def model_harvest_devices(self):
        if self.data_mocking.get_table_data_lenght("harvest_devices", limit=1) > 0:
            print("Harvest devices table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_harvest_devices()
            print("\nHarvest devices table inserted successfully")
            self.report("harvest_devices", count, times)

    def model_precipitation_types(self):
        if self.data_mocking.get_table_data_lenght("prec_types", limit=2) > 1:
            print("Precipitation types table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_precipitation_types()
            print("\nPrecipitation types table inserted successfully")
            self.report("prec_types", count, times)

    def model_weather_metrics(self):
        if self.data_mocking.get_table_data_lenght("weather_metrics", limit=1) > 0:
            print("Weather metrics table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_weather_metrics()
            print("Weather metrics table inserted successfully")
            self.report("weather_metrics", count, times)

    def model_cultivations(self):
        if self.data_mocking.get_table_data_lenght("cultivations", limit=1) > 0:
            print("Cultivation table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_cultivations()
            print("\nCultivation table inserted successfully")
            self.report("cultivations", count, times)

    def model_cultivation_devices(self):
        if self.data_mocking.get_table_data_lenght("cultivation_devices", limit=1) > 0:
            print("Cultivation devices table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_cultivation_devices()
            print("\nCultivation devices table inserted successfully")
            self.report("cultivation_devices", count, times)

    def model_devices_calendars(self):
        if self.data_mocking.get_table_data_lenght("devices_calendars", limit=1) > 0:
            print("Devices calendars table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_devices_calendars()
            print("\nDevices calendars table inserted successfully")
            self.report("devices_calendars", count, times)

    def model_expenses(self, **args):
        if self.data_mocking.get_table_data_lenght("expenses", limit=1) > 0:
            print("Expenses table had been inserted before this run.")
        else:
            times, count = self.data_mocking.insert_expenses(
                args.get("duration", duration)
            )
            print("\nExpenses table inserted successfully")
            self.report("expenses", count, times)
