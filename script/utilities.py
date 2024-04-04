import random
import datetime


def calculate_insert_quantity(quantity):
    if quantity > 24:
        insert_quantity = random.randint(0, quantity // 10)
    elif quantity > 11:
        insert_quantity = random.randint(0, quantity // 6)
    elif quantity > 7:
        insert_quantity = random.randint(0, quantity // 4)
    elif quantity > 3:
        insert_quantity = random.randint(0, quantity // 3)
    else:
        insert_quantity = random.randint(0, quantity)
    return insert_quantity


def get_growth_duration_and_min_max_yield_by_product_name(products, product_name):
    for product in products:
        if product[0] == product_name:
            return product[6], product[7], product[10], product[11]
    return None


def random_date_within_months(min_month, max_month, year):
    min_date = datetime.datetime(year, min_month, 1)
    max_date = datetime.datetime(year, max_month, 1) + datetime.timedelta(days=30)
    return min_date + (max_date - min_date) * random.random()


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