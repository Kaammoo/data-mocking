from utility import DataMocking

def run():
    project = DataMocking()
    project.insert_users(20)
    print("DOne 1")
    project.insert_measurement_units()
    print("DOne 2")
    project.fields()
    print("DOne 3")
    project.insert_precipitation_types()
    print("DOne 4")
    project.insert_product_type()
    print("DOne 5")
    project.insert_products()
    print("DOne 6")
    project.insert_records()
    print("DOne 7")
    
    project.insert_expense_categories()
    print("DOne 8")
    project.insert_expenses()
    
    print("DOne 9")
    project.insert_plantings()
    print("DOne 10")
    project.insert_harvest()
    print("DOne 11")
    project.insert_revenue()
    print("DOne 12")
    project.insert_weather_metrics()
    
    
    print("DOne 13")
    project.insert_cultivation()
    print("DOne 14")
    # project.calculate_yield()
    # print("DOne 15")
run()
