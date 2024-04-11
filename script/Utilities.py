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


def get_crop_count(field_size,min_crop_count,max_crop_count):
    if workers_count > 9:
        workers_count -= random.randint(0, 3)
    else:
        workers_count += random.randint(0, 2)
    crop_count = (
        field_size * random.randint(min_crop_count, max_crop_count)
    ) / 1000
    return crop_count