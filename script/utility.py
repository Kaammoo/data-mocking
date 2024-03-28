import random
from faker import Faker
from db import con
from consts import *
import datetime
import time
import pytz


class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()
        self.fake = Faker()

    def get_season(self, month):
        if month in range(3, 6):
            return "Spring"
        elif month in range(6, 9):
            return "Summer"
        elif month in range(9, 12):
            return "Autumn"
        else:
            return "Winter"

    def get_crops_and_months_by_product_name(self, products, product_name):
        for product in products:
            if product[0] == product_name:
                return product[2], product[3], product[4], product[5]
        return None

    def random_date_within_months(self, min_month, max_month, year):
        date_year = datetime.datetime.now().year - year

        month = random.randint(min_month, max_month)
        day = random.randint(1, 28)
        return datetime.datetime(date_year, month, day)

    def get_growth_duration_and_min_max_yield_by_product_name(
        self, products, product_name
    ):
        for product in products:
            if product[0] == product_name:
                return product[6], product[7], product[10], product[11]
        return None

    def insert_users(self, users_per_community):
        self.cursor_obj.execute("SELECT id FROM communities")
        communities = self.cursor_obj.fetchall()[0]
        unique_id = str(int(time.time()))
        for community_id in range(1, 11):
            for _ in range(users_per_community):
                name = self.fake.name()
                email = self.fake.email() + f"_{unique_id}"
                code = random.choice(["94", "98", "93", "33", "91", "99", "55", "95"])
                phone_number = "+374" + code + self.fake.numerify("#######")

                self.cursor_obj.execute(
                    "INSERT INTO users (name, email, phone_number) VALUES (%s, %s, %s) RETURNING id",
                    (name, email, phone_number),
                )
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT * FROM users")
        users = self.cursor_obj.fetchall()
        for community in communities:
            community_id = community[0]
            for user in users:
                user_id = user[0]

                self.cursor_obj.execute(
                    "INSERT INTO users_communities (user_id, community_id) VALUES (%s, %s)",
                    (user_id, community_id),
                )

    def insert_expenses(self):
        self.cursor_obj.execute(
            """
            SELECT p.date, h.date, p.record_id 
            FROM plantings p 
            INNER JOIN harvests h ON p.record_id = h.record_id
        """
        )

        sowing_harvest_record_dates = self.cursor_obj.fetchall()

        for sowing_date, harvest_date, record_id in sowing_harvest_record_dates:
            period = (harvest_date - sowing_date).days

            if period <= 0:
                continue

            for _ in range(5):
                expense_date = sowing_date + datetime.timedelta(
                    days=random.randint(0, period)
                )

                category_id = random.randint(1, 4)
                amount = random.randint(100, 1000)

                self.cursor_obj.execute(
                    """
                    INSERT INTO expenses (record_id, category_id, amount, date)
                    VALUES (%s, %s, %s, %s)
                """,
                    (record_id, category_id, amount, expense_date),
                )

    def calculate_yield(self):
        self.cursor_obj.execute(
            """
            SELECT record_id, avg_temperature, other_factors, water_amount, fertilizer_quantity
            FROM cultivations
        """
        )
        cultivation_records = self.cursor_obj.fetchall()

        for record in cultivation_records:
            (
                record_id,
                avg_temp,
                other_factors,
                avg_water_amount,
                avg_fertilizer_quantity,
            ) = record

            self.cursor_obj.execute(
                """
                SELECT p.name
                FROM records r
                JOIN products p ON r.product_id = p.id
                WHERE r.id = %s
            """,
                (record_id,),
            )
            product_name = self.cursor_obj.fetchone()[0]

            self.cursor_obj.execute(
                """
                SELECT yield
                FROM harvests
                WHERE record_id = %s
            """,
                (record_id,),
            )
            old_yield = self.cursor_obj.fetchone()[0]

            change = 0
            for product in products_armenia:
                if product[0] == product_name:
                    if (
                        avg_fertilizer_quantity - product[12]
                        > avg_fertilizer_quantity - product[13]
                    ):
                        change -= 0.1
                    else:
                        change -= 0.3
                    if avg_temp - product[8] > avg_temp - product[9]:
                        change += 0.3
                    elif avg_temp - product[8] < avg_temp - product[9]:
                        change += 0.1
                    break

            change += other_factors

            new_yield = old_yield - old_yield * change / 100

            self.cursor_obj.execute(
                """
                UPDATE harvests
                SET yield = %s
                WHERE record_id = %s
            """,
                (new_yield, record_id),
            )
        con.commit()
        self.cursor_obj.close()
        con.close()

    def insert_weather_metrics(self):
        self.cursor_obj.execute(
            """
            SELECT p.date, r.community_id
            FROM plantings p
            INNER JOIN records r ON p.record_id = r.id
        """
        )
        sowing_dates_and_communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute(
            """
            SELECT date 
            FROM harvests
        """
        )
        harvest_dates = self.cursor_obj.fetchall()
        for sowing_date, community_id in sowing_dates_and_communities:
            corresponding_harvest_date = None
            for (harvest_date,) in harvest_dates:
                if harvest_date > sowing_date:
                    corresponding_harvest_date = harvest_date
                    break
            if corresponding_harvest_date:
                current_date = sowing_date
                while current_date < corresponding_harvest_date:
                    month = current_date.month
                    current_season = self.get_season(month)
                    if current_season == "Summer":
                        precipitation_types = ["rain"]
                        temp_range = (10, 40)
                        humidity_range = (30, 80)
                    elif current_season == "Winter":
                        precipitation_types = ["snow", "hail"]
                        temp_range = (-10, 5)
                        humidity_range = (20, 70)
                    else:
                        precipitation_types = ["rain", "snow", "hail"]
                        temp_range = (-5, 30)
                        humidity_range = (40, 70)
                    humidity = random.randint(*humidity_range)
                    temperature = random.randint(*temp_range)
                    prec_type = random.choice(precipitation_types)
                    if prec_type == "rain":
                        rain_drop = random.randint(10, 100)
                    else:
                        rain_drop = 0
                    self.cursor_obj.execute(
                        """
                        INSERT INTO weather_metrics (community_id, rain_drop, humidity, temperature, prec_type_id, date)
                        VALUES (%s, %s, %s, %s, (SELECT id FROM prec_types WHERE name = %s), %s)
                    """,
                        (
                            community_id,
                            rain_drop,
                            humidity,
                            temperature,
                            prec_type,
                            current_date,
                        ),
                    )

                    current_date += datetime.timedelta(days=1)

    def insert_product_type(self):
        self.cursor_obj.execute("SELECT id FROM products")
        product_type_data = [("vegetables",), ("cereal",)]
        for product_type in product_type_data:
            sql = "INSERT INTO product_types (type) VALUES (%s)"
            self.cursor_obj.execute(sql, product_type)

    def insert_products(self):
        self.cursor_obj.execute(
            "SELECT id FROM product_types WHERE type = 'vegetables'"
        )
        vegetable_id = self.cursor_obj.fetchone()
        self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'cereal'")
        cereal_id = self.cursor_obj.fetchone()
        for product in products_armenia:
            if "vegetable" in product[1].lower():
                type_id = vegetable_id
            else:
                type_id = cereal_id

            self.cursor_obj.execute(
                "INSERT INTO products (type_id, name, description) VALUES (%s, %s, %s)",
                (type_id, product[0], product[1]),
            )

    def insert_measurement_units(self):
        measurements = [
            ("liter", "volume"),
            ("meter", "length"),
            ("Celsius", "temperature"),
            ("g/m^3", "density"),
        ]
        for measurement in measurements:
            self.cursor_obj.execute(
                "INSERT INTO measurement_units (value, type) VALUES (%s,%s)",
                (measurement[0], measurement[1]),
            )

    def insert_revenue(self):
        self.cursor_obj.execute("SELECT id, yield, date FROM harvests")
        harvests = self.cursor_obj.fetchall()

        for harvest_id, yield_amount, harvest_date in harvests:
            amount = round(yield_amount * random.uniform(10, 50), 2)
            revenue_date = harvest_date + datetime.timedelta(days=random.randint(5, 10))

            self.cursor_obj.execute(
                """
                INSERT INTO revenues (harvest_id, amount, date)
                VALUES (%s, %s, %s)
            """,
                (harvest_id, amount, revenue_date),
            )

    def insert_records(self):
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT * FROM products")
        products = self.cursor_obj.fetchall()
        for community in communities:
            community_name = community[1] + "%"
            community_id = community[0]
            self.cursor_obj.execute(
                f"SELECT * FROM fields WHERE name LIKE '{community_name}'"
            )
            fields = self.cursor_obj.fetchall()
            for field in fields:
                unique_products = random.sample(products, 5)
                for product in unique_products:
                    self.cursor_obj.execute(
                        "INSERT INTO records (community_id, field_id, product_id) VALUES (%s, %s, %s)",
                        (community_id, field[0], product[0]),
                    )

    def insert_precipitation_types(self):
        precipitation_types = [("rain",), ("snow",), ("hail",)]

        for prec_type in precipitation_types:
            self.cursor_obj.execute(
                "INSERT INTO prec_types (name) VALUES (%s)", prec_type
            )

    def insert_expense_categories(self):
        expense_categories = [
            ("fertilizer",),
            ("equipment costs",),
            ("seed purchase",),
            ("employee expenses",),
        ]
        for category in expense_categories:
            self.cursor_obj.execute(
                "INSERT INTO expense_categories (name) VALUES (%s)", category
            )

    def fields(self):
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()

        for community in communities:
            for i in range(random.randint(min_field_count, max_field_count)):
                field_name = f"{community[1]}_field{i + 1}"
                field_size = random.randint(min_field_size, max_field_size)
                self.cursor_obj.execute(
                    "INSERT INTO fields (name, size, measurement_id) VALUES \
                    (%s, %s, %s)",
                    (field_name, field_size, 4),
                )

        for community in communities:
            community_name = community[1] + "%"
            community_id = community[0]
            self.cursor_obj.execute(
                f"SELECT * FROM fields WHERE name LIKE '{community_name}'"
            )
            fields = self.cursor_obj.fetchall()
            for field in fields:
                self.cursor_obj.execute(
                    "INSERT INTO fields_communities (field_id, community_id) VALUES\
                    (%s, %s)",
                    (field[0], community_id),
                )

    def insert_portable_devices(self):
        for device_name in portable_device_data:
            self.cursor_obj.execute(
                "INSERT INTO portable_devices (name) VALUES (%s)", (device_name,)
            )

    def insert_cultivation(self):
        self.cursor_obj.execute(
            """
            SELECT p.date, h.date, p.record_id 
            FROM plantings p 
            INNER JOIN harvests h ON p.record_id = h.record_id
        """
        )
        sowing_harvest_record_dates = self.cursor_obj.fetchall()
        print(sowing_harvest_record_dates)
        # 2020-03-22 00:00:00.000 +0400
        for sowing_date, harvest_date, record_id in sowing_harvest_record_dates:
            start_date = sowing_date.strftime("%Y-%m-%d %H:%M:%S.%s %z")
            end_date = harvest_date.strftime("%Y-%m-%d %H:%M:%S.%s %z")

            self.cursor_obj.execute(
                """
                SELECT community_id 
                FROM records 
                WHERE id = %s
            """,
                (record_id,),
            )
            community_id = self.cursor_obj.fetchone()[0]
            print(f"{community_id = }")
            print(f"{start_date = }")
            print(f"{end_date = }")

            query = f"""
                SELECT AVG(humidity), AVG(temperature) 
                FROM weather_metrics 
                WHERE community_id = {community_id} AND date BETWEEN '{start_date}' AND '{end_date}'
            """
            print(f"{query = }")
            self.cursor_obj.execute(query)
            avg_humidity, avg_temp = self.cursor_obj.fetchone()
            print(f"{avg_humidity = }")
            print(f"{avg_temp = }")

            self.cursor_obj.execute(
                """
                SELECT AVG(rain_drop)
                FROM weather_metrics
                WHERE community_id = %s AND date BETWEEN %s AND %s
            """,
                (community_id, start_date, end_date),
            )
            water_amount = self.cursor_obj.fetchone()[0]

            self.cursor_obj.execute(
                """
                SELECT AVG(workers_quantity)
                FROM plantings
                WHERE record_id = %s
            """,
                (record_id,),
            )
            workers_quantity = self.cursor_obj.fetchone()[0]
            print(f"{workers_quantity = }")
            print(f"{avg_humidity = }")
            print(f"{avg_temp = }")
            print(f"{water_amount = }")
            
            if (
                avg_humidity is not None
                and avg_temp is not None
                and water_amount is not None
            ):
                avg_humidity = round(avg_humidity, 2)
                avg_temp = round(avg_temp, 2)
                water_amount = round(water_amount, 2)

                other_factors = round(random.uniform(0.1, 0.49), 2)
                for product in products_armenia:
                    min_count_fertilizer = product[12]
                    max_count_fertilizer = product[13]
                    fertilizer_quantity = round(
                        random.randint(min_count_fertilizer, max_count_fertilizer)
                    )

                    self.cursor_obj.execute(
                        """
                        INSERT INTO cultivations (record_id, start_date, end_date, avg_humidity, avg_temperature, fertilizer_quantity, water_amount, workers_quantity, other_factors)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        ),
                    )
            else:
                raise("Insert Error: line 433")
        con.commit()
        self.cursor_obj.close()
        con.close()

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
                    years_range = tuple(
                        x for x in years_duration if x != used_records[field_id][0]
                    )
                    rand_year = random.choice(years_range)
                    used_records[field_id].append(rand_year)
                else:
                    years_range = tuple(
                        item
                        for item in years_duration
                        if item not in used_records[field_id]
                    )
                    rand_year = random.choice(years_range)
                    used_records[field_id].append(rand_year)
            else:
                rand_year = random.choice(years_duration)
                used_records[field_id] = [rand_year]
            self.cursor_obj.execute(f"SELECT name FROM products WHERE id = {record[1]}")
            product_name = self.cursor_obj.fetchone()[0]

            crop_info = self.get_crops_and_months_by_product_name(
                products_armenia, product_name
            )

            if crop_info is None:
                continue
            min_crop_count, max_crop_count, min_month, max_month = crop_info
            random_date_generated = self.random_date_within_months(
                min_month, max_month, rand_year
            )
            record_id = record[0]
            self.cursor_obj.execute(f"SELECT size FROM fields WHERE id = {field_id}")
            field_size = self.cursor_obj.fetchone()[0]
            workers_count = field_size // 100
            if workers_count > 9:
                workers_count -= random.randint(0, 3)
            else:
                workers_count += random.randint(0, 2)
            crop_count = (
                field_size * random.randint(min_crop_count, max_crop_count)
            ) / 1000
            self.cursor_obj.execute(
                "INSERT INTO plantings (record_id, crop_quantity, date, workers_quantity) VALUES (%s, %s, %s, %s)",
                (record_id, crop_count, random_date_generated, workers_count),
            )

    def insert_harvest(self):
        self.cursor_obj.execute("SELECT record_id, crop_quantity, date FROM plantings")
        plantings_data = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
        records = self.cursor_obj.fetchall()
        self.cursor_obj.execute(f"SELECT id, name FROM products")
        products = self.cursor_obj.fetchall()
        
        # Define UTC timezone
        utc = pytz.UTC
        
        for planting in plantings_data:
            planting_record_id = planting[0]
            planting_date = planting[2].replace(tzinfo=utc)  # Make planting_date aware
            
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
                                
                            # Make harvest_date aware with UTC timezone
                            harvest_date = datetime.datetime(harvest_year, harvest_month, harvest_day, tzinfo=utc)
                            
                            # Check if the calculated harvest date is before the planting date
                            if harvest_date < planting_date:
                                # Handle the case where the harvest date is earlier than the planting date
                                continue
                            
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