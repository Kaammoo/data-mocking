# utilities.py
import random
import datetime
from DataMocking import cursor_obj
from faker import Faker

fake = Faker()

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


def generate_user_data(unique_id):
        name = fake.name()
        email = fake.email() + f"_{unique_id}"
        code = random.choice(["94", "98", "93", "33", "91", "99", "55", "95"])
        phone_number = "+374" + code + fake.numerify("#######")
        return name, email, phone_number


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


def get_product_name_by_id(product_id):
    cursor_obj.execute(f"SELECT name FROM products WHERE id = {product_id}")
    return cursor_obj.fetchone()[0]


def get_planting_year(record_id):
    cursor_obj.execute(f"SELECT YEAR(date) FROM plantings WHERE record_id = {record_id}")
    return cursor_obj.fetchone()[0]


def get_field_size_by_id(field_id):
    cursor_obj.execute(f"SELECT size FROM fields WHERE id = {field_id}")
    return cursor_obj.fetchone()[0]


def get_workers_count_by_field_id(field_id):
    field_size = get_field_size_by_id(field_id)
    workers_count = field_size // 10
    if workers_count > 9:
        workers_count -= random.randint(0, 3)
    else:
        workers_count += random.randint(0, 2)
    return workers_count


def insert_planting_record(record_id, crop_count, random_date_generated, workers_count):
    cursor_obj.execute(
        "INSERT INTO plantings (record_id, crop_quantity, date, workers_quantity) VALUES (%s, %s, %s, %s)",
        (record_id, crop_count, random_date_generated, workers_count),
    )


def insert_planting_record(record_id, crop_count, random_date_generated, workers_count):
    cursor_obj.execute(
        "INSERT INTO plantings (record_id, crop_quantity, date, workers_quantity) VALUES (%s, %s, %s, %s)",
        (record_id, crop_count, random_date_generated, workers_count),
    )


def insert_devices_calendar_record(start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity):
    cursor_obj.execute(
        "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
        (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity),
    )


def get_planting_devices(planting_id):
    cursor_obj.execute(
        f"SELECT portable_devices_communities_id, quantity FROM planting_devices WHERE planting_id = {planting_id}"
    )
    return cursor_obj.fetchall()


def get_cultivation_devices(cultivation_id):
    cursor_obj.execute(
        f"SELECT portable_devices_communities_id, quantity FROM cultivation_devices WHERE cultivation_id = {cultivation_id}"
    )
    return cursor_obj.fetchall()


def get_harvest_devices(harvest_id):
    cursor_obj.execute(
        f"SELECT portable_devices_communities_id, quantity FROM harvest_devices WHERE harvest_id = {harvest_id}"
    )
    return cursor_obj.fetchall()


def insert_cultivation_device(cultivation_id, portable_device_community_id, insert_quantity):
    cursor_obj.execute(
        "INSERT INTO cultivation_devices (cultivation_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
        (cultivation_id, portable_device_community_id, insert_quantity),)


def insert_harvest(record_id, product_yield, harvest_date, field_size, workers_count):
    cursor_obj.execute(
        "INSERT INTO harvests (record_id, yield, date, acres_cut, workers_quantity) VALUES (%s, %s, %s, %s, %s)",
            (
            record_id,
            product_yield,
            harvest_date,
            field_size,
            workers_count,
            ),
        )


def insert_cultivation(record_id, start_date, end_date, avg_humidity, avg_temp, fertilizer_quantity, water_amount, workers_quantity, other_factors, irrigation_hours, fertilizing_hours, soil_compaction_hours):
    cursor_obj.execute(
        """
        INSERT INTO cultivations (record_id, start_date, end_date, avg_humidity, avg_temperature, fertilizer_quantity, water_amount, workers_quantity, other_factors, irrigation_hours, fertilizing_hours, soil_compaction_hours)
        VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)
        """,
        (
            record_id,
            start_date,
            end_date,
            avg_humidity,
            avg_temp,
            fertilizer_quantity,
            water_amount,
            workers_quantity,
            other_factors,
            irrigation_hours,
            fertilizing_hours,
            soil_compaction_hours,
        ),
    )


def insert_portable_device(device_name):
    cursor_obj.execute(
        "INSERT INTO portable_devices (name) VALUES (%s)", (device_name,)
    )


def insert_field_community(field_id, community_id):
    cursor_obj.execute(
        "INSERT INTO fields_communities (field_id, community_id) VALUES\
            (%s, %s)",
            (field_id, community_id),
    )


def insert_field(field_name, field_size, measurement_id):
    cursor_obj.execute(
        "INSERT INTO fields (name, size, measurement_id) VALUES \
            (%s, %s, %s)",
            (field_name, field_size, measurement_id),
    )


def insert_user(name, email, phone_number):
    cursor_obj.execute(
        "INSERT INTO users (name, email, phone_number) VALUES (%s, %s, %s) RETURNING id",
        (name, email, phone_number),
    )
    return cursor_obj.fetchone()[0]


def associate_user_with_community(user_id, community_id):
    cursor_obj.execute(
        "INSERT INTO users_communities (user_id, community_id) VALUES (%s, %s)",
        (user_id, community_id),
    )


def insert_expense(record_id, category_id, amount, date):
    cursor_obj.execute(
        """
        INSERT INTO expenses (record_id, category_id, amount, date)
        VALUES (%s, %s, %s, %s)
        """,
        (record_id, category_id, amount, date),
    )

def portable_device_community(portable_device_id, community_id, input_device_count):
    cursor_obj.execute(
        """
        INSERT INTO portable_devices_communities (portable_device_id, community_id, quantity)
        VALUES (%s, %s, %s)
        """, (portable_device_id, community_id, input_device_count))


def insert_planting_device(planting_id, portable_device_community_id, insert_quantity):
    cursor_obj.execute(
        "INSERT INTO planting_devices (planting_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
        (planting_id, portable_device_community_id, insert_quantity),
    )


def insert_harvest_device(harvest_id, portable_device_community_id, insert_quantity):
    cursor_obj.execute(
        "INSERT INTO harvest_devices (harvest_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
        (harvest_id, portable_device_community_id, insert_quantity),
    )


def generate_insert_quantity(quantity):
    if quantity > 7:
        return random.randint(0, quantity // 4)
    else:
        return random.randint(0, quantity)


def update_new_yield(new_yield, record_id):
    cursor_obj.execute(
        """
        UPDATE harvests
        SET yield = %s
        WHERE record_id = %s
        """,
        (new_yield, record_id),
    )