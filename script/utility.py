import random
from db import con
from consts import *
import faker
from datetime import datetime, timedelta

class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()


    def insert_product_type(self):
        self.cursor_obj.execute("SELECT id FROM products")
        product_type_data = (
            ("fruit",),
            ("vegetables",),
            ("cereal",),
            ("berry",),
        )
        for product_type in product_type_data:
            self.cursor_obj.execute("INSERT INTO product_types (type) VALUES (%s)", product_type)


    def insert_products(self):
        for product in products_armenia:
            self.cursor_obj.execute("INSERT INTO products (name, description) VALUES (%s, %s)", product[:2])


    def insert_fields(self):
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()

        for community in communities:
            for i in range(random.randint(min_field_count, max_field_count)):
                field_name = f"{community[1]}_field{i + 1}"
                field_size = random.randint(min_field_size, max_field_size)
                self.cursor_obj.execute("INSERT INTO fields (name, size, measurement_id) VALUES \
                    (%s, %s, %s)", (field_name, field_size, 4))

        for community in communities:
            community_name = community[1] + "%"
            community_id = community[0]
            self.cursor_obj.execute(f"SELECT * FROM fields WHERE name LIKE '{community_name}'")
            fields = self.cursor_obj.fetchall()
            for field in fields:
                self.cursor_obj.execute("INSERT INTO fields_communities (field_id, community_id) VALUES\
                    (%s, %s)", (field[0], community_id))


    def insert_records(self):
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT * FROM products")
        products = self.cursor_obj.fetchall()
        for community in communities:
            community_name = community[1] + "%"
            community_id = community[0]
            self.cursor_obj.execute(f"SELECT * FROM fields WHERE name LIKE '{community_name}'")
            fields = self.cursor_obj.fetchall()
            for field in fields:
                self.cursor_obj.execute("INSERT INTO records (community_id, field_id, product_id) VALUES (%s, %s, %s)",\
                                        (community_id, field[0], random.choice(products)[0]))


    def insert_portable_devices(self):
        for device_name in portable_device_data:
            self.cursor_obj.execute("INSERT INTO portable_devices (name) VALUES (%s)", (device_name,))



    def get_crops_and_months_by_product_name(self, products, product_name):
        for product in products:
            if product[0] == product_name:
                return product[2], product[3], product[4], product[5]
        return None 


    def random_date_within_months(self, min_month, max_month, year):
        date_year = datetime.now().year - year
        month = random.randint(min_month, max_month)
        day = random.randint(1, 28)
        return datetime(date_year, month, day)


    def insert_plantings(self):
        self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
        records = self.cursor_obj.fetchall()
        used_records = {}
        years_duration = tuple((i for i in range(1, duration + 1)))
        for record in records:
            field_id = record[2]
            if field_id in used_records:
                if len(used_records[field_id]) >= duration:
                    continue
                elif len(used_records[field_id]) == 1:
                    years_range = (x for x in years_duration if x != used_records[field_id])
                    rand_year = random.choice(years_range)
                    used_records[field_id] += (rand_year,)
                else:
                    years_range = tuple(item for item in years_duration if item not in used_records[field_id])
                    rand_year = random.choice(years_range)
                    used_records[field_id].add(rand_year)
            else:
                rand_year = random.choice(years_duration)
                used_records[field_id] = (rand_year,)
            self.cursor_obj.execute(f"SELECT name FROM products WHERE id = {record[1]}")
            product_name = self.cursor_obj.fetchone()[0]
            crop_info = self.get_crops_and_months_by_product_name(products_armenia, product_name)
            if crop_info is None:
                continue
            min_crop_count, max_crop_count, min_month, max_month = crop_info
            random_date_generated = self.random_date_within_months(min_month, max_month, rand_year)
            record_id = record[0]
            self.cursor_obj.execute(f"SELECT size FROM fields WHERE id = {field_id}")
            field_size = self.cursor_obj.fetchone()[0]
            workers_count = field_size // 100
            if workers_count > 9:
                workers_count -= random.randint(0, 3)
            else:
                workers_count += random.randint(0, 2)
            crop_count = (field_size * random.randint(min_crop_count, max_crop_count)) / 1000

            self.cursor_obj.execute("INSERT INTO plantings (record_id, crop_quantity, date, workers_quantity) VALUES (%s, %s, %s, %s)",\
                (record_id, crop_count, random_date_generated, workers_count))

        
        con.commit()
        self.cursor_obj.close()
        con.close()


