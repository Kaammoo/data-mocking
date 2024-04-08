from DataMocking import DataMocking

def run():
    project = DataMocking()
    project.insert_users()
    print("Users table inserted successfully")
    
    project.insert_measurement_units()
    print("Measurement units table inserted successfully")
    
    project.fields()
    print("Fields table inserted successfully")
    
    project.insert_precipitation_types()
    print("Precipitation types table inserted successfully")
    
    project.insert_product_types()
    print("Product types table inserted successfully")
    
    project.insert_products()
    print("Products table inserted successfully")
    
    project.insert_records()
    print("Records table inserted successfully")

    project.insert_expense_categories()
    print("Expense categories table inserted successfully")
    
    project.insert_expenses()
    print("Expenses table inserted successfully")
    
    project.insert_portable_devices()
    print("Portable devices table inserted successfully")
    
    project.insert_portable_devices_communities()
    print("Portable devices communities table inserted successfully")
    
    project.insert_plantings()
    print("Plantings table inserted successfully")
    
    project.insert_harvest()
    print("Harvest table inserted successfully")
    
    project.insert_planting_devices()
    print("Planting devices table inserted successfully")
    
    project.insert_harvest_devices()
    print("Harvest devices table inserted successfully")
    
    project.insert_revenues()
    print("Revenues table inserted successfully")
    
    project.insert_weather_metrics()
    print("Weather metrics table inserted successfully")

    project.insert_cultivations()
    print("Cultivations table inserted successfully")
    
    project.calculate_yields()
    print("Yields calculated successfully")
    
    project.insert_cultivation_devices()
    print("Cultivation devices table inserted successfully")
    
    project.insert_devices_calendars()
    print("Devices calendars table inserted successfully")

run()
