import random
from db import con
from consts import *
import faker
import datetime

class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()


    def insert_product_type(self):
        self.cursor_obj.execute("SELECT id FROM products")
        product_type_data = [
            ("vegetables",),
            ("cereal",)
        ]
        for product_type in product_type_data:
            sql = "INSERT INTO product_types (type) VALUES (%s)"
            self.cursor_obj.execute(sql, product_type)


    def insert_products(self):
        self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'vegetables'")
        vegetable_id = self.cursor_obj.fetchone()
        self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'cereal'")
        cereal_id = self.cursor_obj.fetchone()
        for product in products_armenia:
            if "vegetable" in product[1].lower():
                type_id = vegetable_id
            else:
                type_id = cereal_id
            
            self.cursor_obj.execute("INSERT INTO products (type_id, name, description) VALUES (%s, %s, %s)",\
                (type_id, product[0], product[1]))


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
                unique_products = random.sample(products, 5)
                for product in unique_products:
                    self.cursor_obj.execute("INSERT INTO records (community_id, field_id, product_id) VALUES (%s, %s, %s)", \
                                        (community_id, field[0], product[0]))


    def insert_portable_devices(self):
        for device_name in portable_device_data:
            self.cursor_obj.execute("INSERT INTO portable_devices (name) VALUES (%s)", (device_name,))


    def get_crops_and_months_by_product_name(self, products, product_name):
        for product in products:
            if product[0] == product_name:
                return product[2], product[3], product[4], product[5]
        return None


    def get_growth_duration_and_min_max_yield_by_product_name(self, products, product_name):
        for product in products:
            if product[0] == product_name:
                return product[6], product[7], product[10], product[11]
        return None


    def random_date_within_months(self, min_month, max_month, year):
        date_year = datetime.datetime.now().year - year
        month = random.randint(min_month, max_month)
        day = random.randint(1, 28)
        return datetime.datetime(date_year, month, day)


    def insert_plantings(self):
        self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
        records = self.cursor_obj.fetchall()
        used_records = {}
        years_duration = tuple((i for i in range(1, duration + 1)))
        for record in records:
            field_id = record[2]
            if field_id in used_records.keys():
                if len(used_records[field_id]) >= duration:
                    continue
                elif len(used_records[field_id]) == 1:
                    years_range = tuple(x for x in years_duration if x != used_records[field_id][0])
                    rand_year = random.choice(years_range)
                    used_records[field_id].append(rand_year)
                else:
                    years_range = tuple(item for item in years_duration if item not in used_records[field_id])
                    rand_year = random.choice(years_range)
                    used_records[field_id].append(rand_year)
            else:
                rand_year = random.choice(years_duration)
                used_records[field_id] = [rand_year]
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
            workers_count = field_size // 10
            if workers_count > 9:
                workers_count -= random.randint(0, 2)
            else:
                workers_count += random.randint(0, 2)
            crop_count = (field_size * random.randint(min_crop_count, max_crop_count)) / 1000
            self.cursor_obj.execute("INSERT INTO plantings (record_id, crop_quantity, date, workers_quantity) VALUES (%s, %s, %s, %s)",
                                    (record_id, crop_count, random_date_generated, workers_count))


    def insert_harvest(self):
        self.cursor_obj.execute("SELECT record_id, crop_quantity, date FROM plantings")
        plantings_data = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
        records = self.cursor_obj.fetchall()
        self.cursor_obj.execute(f"SELECT id, name FROM products")
        products = self.cursor_obj.fetchall()
        for planting in plantings_data:
            planting_record_id = planting[0]
            planting_date = planting[2]
            for record in records:
                record_id = record[0]
                if planting_record_id == record_id:
                    record_product_id = record[1]
                    for product in products:
                        product_id = product[0]
                        if record_product_id == product_id:
                            product_name = product[1]
                            min_growth_duration, max_growth_duration, min_yield, max_yield = \
                                self.get_growth_duration_and_min_max_yield_by_product_name(products_armenia, product_name)
                            rand_day = random.randint(min_growth_duration, max_growth_duration)
                            harvest_date_day = planting_date.day + rand_day
                            planting_date_month = planting_date.month
                            harvest_year = planting_date.year
                            if harvest_date_day > 28:
                                harvest_day = harvest_date_day % 28
                                harvest_month = harvest_date_day // 28
                            if (planting_date_month + harvest_month) > 12:
                                harvest_year += 1
                                harvest_month = (planting_date_month + harvest_month) % 12
                            if harvest_day == 0:
                                harvest_day += 1
                            harvest_date = datetime.datetime(harvest_year, harvest_month, harvest_day)
                            field_id = record[2]
                            self.cursor_obj.execute(f"SELECT size FROM fields WHERE id = {field_id}")
                            field_size = self.cursor_obj.fetchone()[0]
                            workers_count = field_size // 10
                            if workers_count > 9:
                                workers_count -= random.randint(0, 2)
                            else:
                                workers_count += random.randint(0, 2)
                            product_yield = field_size * random.randint(min_yield, max_yield)
                            self.cursor_obj.execute("INSERT INTO harvests (record_id, yield, date, acres_cut, workers_quantity) VALUES (%s, %s, %s, %s, %s)",\
                                    (record_id, product_yield, harvest_date, field_size, workers_count))


    def insert_portable_devices_communities(self):
        self.cursor_obj.execute("SELECT id FROM communities")
        communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, name FROM portable_devices")
        portable_devices = self.cursor_obj.fetchall()
        for community in communities:
            community_id = community[0]
            for portable_device in portable_devices:
                portable_device_id = portable_device[0]
                portable_device_name = portable_device[1]
                if "Shovel" == portable_device_name or "Rake" == portable_device_name or \
                    portable_device_name == "Spade" or portable_device_name == "Hoe":
                    input_device_count = random.randint(100, 200)
                elif "Tractor" in portable_device_name:
                    input_device_count = random.randint(10, 20)
                elif "Combine" in portable_device_name:
                    input_device_count = random.randint(10, 20)
                else:
                    input_device_count = random.randint(1, 9)
                self.cursor_obj.execute("INSERT INTO portable_devices_communities (portable_device_id, community_id, quantity) VALUES (%s, %s, %s)",\
                                    (portable_device_id, community_id, input_device_count))


    def insert_planting_devices(self):
        self.cursor_obj.execute("SELECT id FROM plantings")
        plantings = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, quantity FROM portable_devices_communities")
        portable_devices_communities = self.cursor_obj.fetchall()
        for planting in plantings:
            planting_id = planting[0]
            for portable_device_community in portable_devices_communities:
                portable_device_community_id = portable_device_community[0]
                portable_device_community_quantity = portable_device_community[1]
                if portable_device_community_quantity > 7:
                    insert_quantity = random.randint(0, portable_device_community_quantity // 4)
                else:
                    insert_quantity = random.randint(0, portable_device_community_quantity)
                self.cursor_obj.execute("INSERT INTO planting_devices (planting_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",\
                                    (planting_id, portable_device_community_id, insert_quantity))


    def insert_harvest_devices(self):
        self.cursor_obj.execute("SELECT id FROM harvests")
        harvests = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, quantity FROM portable_devices_communities")
        portable_devices_communities = self.cursor_obj.fetchall()
        for harvest in harvests:
            harvest_id = harvest[0]
            for portable_device_community in portable_devices_communities:
                portable_device_community_id = portable_device_community[0]
                portable_device_community_quantity = portable_device_community[1]
                if portable_device_community_quantity > 7:
                    insert_quantity = random.randint(0, portable_device_community_quantity // 4)
                else:
                    insert_quantity = random.randint(0, portable_device_community_quantity)
                self.cursor_obj.execute("INSERT INTO harvest_devices (harvest_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",\
                                    (harvest_id, portable_device_community_id, insert_quantity))
        con.commit()
        self.cursor_obj.close()
        con.close()