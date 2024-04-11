import datetime
import random

def get_season( month):
    if month in range(3, 6):
        return "Spring"
    elif month in range(6, 9):
        return "Summer"
    elif month in range(9, 12):
        return "Autumn"
    else:
        return "Winter"
    
    
def get_crops_and_months_by_product_name( products, product_name):
    for product in products:
        if product[0] == product_name:
            return product[2], product[3], product[4], product[5]
    return None


def random_date_within_months( min_month, max_month, year):
    date_year = datetime.datetime.now().year - year

    month = random.randint(min_month, max_month)
    day = random.randint(1, 28)
    return datetime.datetime(date_year, month, day)

def get_growth_duration_and_min_max_yield_by_product_name(
     products, product_name
):
    for product in products:
        if product[0] == product_name:
            return product[6], product[7], product[10], product[11]
    return None

def get_input_device_count(portable_device_name, devices_weights):
    if (
        portable_device_name == "Shovel"
        or portable_device_name == "Rake"
        or portable_device_name == "Spade"
        or portable_device_name == "Hoe"
    ):
        input_device_count = random.randint(*devices_weights["hand tool"])
    elif "Tractor" in portable_device_name or "Combine" in portable_device_name:
        input_device_count = random.randint(*devices_weights["Tractor or Combine"])
    else:
        input_device_count = random.randint(*devices_weights["Truck"])
        
    return input_device_count


def get_crop_count(field_size,min_crop_count,max_crop_count):
    if workers_count > 9:
        workers_count -= random.randint(0, 3)
    else:
        workers_count += random.randint(0, 2)
    crop_count = (
        field_size * random.randint(min_crop_count, max_crop_count)
    ) / 1000
    return crop_count


def get_insert_quantity(portable_device_community_quantity):
    if 8 > portable_device_community_quantity > 3:
        insert_quantity = random.randint(
            0, portable_device_community_quantity // 3
        )
    elif 12 > portable_device_community_quantity > 7:
        insert_quantity = random.randint(
            0, portable_device_community_quantity // 4
        )
    elif 25 > portable_device_community_quantity > 11:
        insert_quantity = random.randint(
            0, portable_device_community_quantity // 6
        )
    elif portable_device_community_quantity > 24:
        insert_quantity = random.randint(
            0, portable_device_community_quantity // 10
        )
    else:
        insert_quantity = random.randint(
            0, portable_device_community_quantity
        )
    return insert_quantity


def tables():
    print("1: Users")
    print("2: Measurement units")
    print("3: Fields")
    print("4: Precipitation types")
    print("5: Product types")
    print("6: Products")
    print("7: Records")
    print("8: Expense categories")
    print("9: Portable devices")
    print("10: Portable devices communities")
    print("11: Plantings")
    print("12: Harvest")
    print("13: Planting devices")
    print("14: Harvest devices")
    print("15: Expenses")
    print("16: Revenues")
    print("17: Weather metrics")
    print("18: Cultivations")
    print("19: Calculate yields")
    print("20: Cultivation devices")
    print("21: Devices calendars")
    
def get_amount(category_id):
    amount = random.randint(100, 1000)
    return amount

