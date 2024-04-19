import datetime
import random
import yaml

def get_season(month):
    if month in range(3, 6):
        return "Spring"
    elif month in range(6, 9):
        return "Summer"
    elif month in range(9, 12):
        return "Autumn"
    else:
        return "Winter"


def get_crops_and_months_by_product_name(products, product_name):
    for product in products:
        if product[0] == product_name:
            return product[2], product[3], product[4], product[5]
    return None


def random_date_within_months(min_month, max_month, year):
    date_year = datetime.datetime.now().year - year

    month = random.randint(min_month, max_month)
    day = random.randint(1, 28)
    return datetime.datetime(date_year, month, day)


def get_growth_duration_and_min_max_yield_by_product_name(products, product_name):
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


def get_crop_count(field_size, min_crop_count, max_crop_count, workers_count):
    if workers_count > 9:
        workers_count -= random.randint(0, 3)
    else:
        workers_count += random.randint(0, 2)
    crop_count = (field_size * random.randint(min_crop_count, max_crop_count)) / 1000
    return crop_count


def get_insert_quantity(portable_device_community_quantity):
    if 8 > portable_device_community_quantity > 3:
        insert_quantity = random.randint(0, portable_device_community_quantity // 3)
    elif 12 > portable_device_community_quantity > 7:
        insert_quantity = random.randint(0, portable_device_community_quantity // 4)
    elif 25 > portable_device_community_quantity > 11:
        insert_quantity = random.randint(0, portable_device_community_quantity // 6)
    elif portable_device_community_quantity > 24:
        insert_quantity = random.randint(0, portable_device_community_quantity // 10)
    else:
        insert_quantity = random.randint(0, portable_device_community_quantity)
    return insert_quantity


def tables():
    print(
        """
    1: Plantings
    2: Harvests
    3: Cultivations
    """
    )


def get_amount(category_id):
    amount = random.randint(100, 1000)
    return amount


def get_change_count(products_armenia, product_name, avg_fertilizer_quantity, avg_temp):
    change = 0
    for product in products_armenia:
        product_ith_name = product[0]
        min_fertilizer_quantity = product[12]
        max_fertilizer_quantity = product[13]
        min_temp_that_product_need = product[8]
        max_temp_that_product_need = product[8]
        if product_ith_name == product_name:

            if (
                avg_fertilizer_quantity - min_fertilizer_quantity
                > avg_fertilizer_quantity - max_fertilizer_quantity
            ):
                change -= 0.1
            else:
                change -= 0.3
            if (
                avg_temp - min_temp_that_product_need
                > avg_temp - max_temp_that_product_need
            ):
                change += 0.3
            elif (
                avg_temp - min_temp_that_product_need
                < avg_temp - max_temp_that_product_need
            ):
                change += 0.1
            break
    return change


def get_wether_date(current_season, month):
    if current_season == "Summer":
        precipitation_types = ["rain", "without_prec"]
        weights = [
            0.3,
            0.7,
        ]  # Adjust the weights according to your preference
        temp_range = (15, 30)
        humidity_range = (30, 80)
    elif current_season == "Winter":
        precipitation_types = ["snow", "hail", "without_prec"]
        weights = [
            0.2,
            0.1,
            0.7,
        ]  # Adjust the weights according to your preference
        temp_range = (-10, 5)
        humidity_range = (20, 70)
    elif current_season == "Spring" or month == 9 or month == 10:
        precipitation_types = ["rain", "hail", "without_prec"]
        weights = [
            0.4,
            0.01,
            0.59,
        ]  # Adjust the weights according to your preference
        temp_range = (10, 20)
        humidity_range = (20, 60)
    else:
        precipitation_types = ["rain", "snow", "hail", "without_prec"]
        weights = [
            0.20,
            0.30,
            0.1,
            0.4,
        ]  # Adjust the weights according to your preference
        temp_range = (0, 15)
        humidity_range = (40, 70)
    return precipitation_types, weights, temp_range, humidity_range


def read_file(file_path):
    try:
        with open(file_path, "r") as file:
            file_contents = file.read()
            print(type(file_contents))
            return file_contents
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")


def read_config_yaml(file_path):
    with open(file_path, 'r') as file:
        prime_service = yaml.safe_load(file)

    return prime_service
