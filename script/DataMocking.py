import random
from faker import Faker
from db import con
from consts import *
from configs import *
from utilities import *
import datetime
from time import time


class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()
        self.fake = Faker()


    def insert_users(self):
        self.cursor_obj.execute("SELECT id FROM communities")
        communities = self.cursor_obj.fetchall()[0]
        users_per_community = random.randint(
            min_users_per_community, max_users_per_community
        )
        unique_id = str(int(time()))
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

    def insert_portable_devices_communities(self):
        # Fetching necessary data
        self.cursor_obj.execute("SELECT id FROM communities")
        communities = self.cursor_obj.fetchall()

        self.cursor_obj.execute("SELECT id, name FROM portable_devices")
        portable_devices = self.cursor_obj.fetchall()

        # Define input device counts based on device names
        device_counts = {
            "Shovel": (100, 200),
            "Rake": (100, 200),
            "Spade": (100, 200),
            "Hoe": (100, 200),
            "Tractor": (10, 20),
            "Combine": (10, 20)
        }

        for community_id, in communities:
            for portable_device_id, portable_device_name in portable_devices:
                input_device_count = random.randint(1, 9)
                for device_name, count_range in device_counts.items():
                    if device_name in portable_device_name:
                        input_device_count = random.randint(*count_range)
                        break

                # Insert into portable_devices_communities
                self.cursor_obj.execute("""
                    INSERT INTO portable_devices_communities (portable_device_id, community_id, quantity)
                    VALUES (%s, %s, %s)
                """, (portable_device_id, community_id, input_device_count))



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
                    insert_quantity = random.randint(
                        0, portable_device_community_quantity // 4
                    )
                else:
                    insert_quantity = random.randint(
                        0, portable_device_community_quantity
                    )
                if insert_quantity > 0:
                    self.cursor_obj.execute(
                        "INSERT INTO planting_devices (planting_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
                        (planting_id, portable_device_community_id, insert_quantity),
                    )

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
                    insert_quantity = random.randint(
                        0, portable_device_community_quantity // 4
                    )
                else:
                    insert_quantity = random.randint(
                        0, portable_device_community_quantity
                    )
                if insert_quantity > 0:

                    self.cursor_obj.execute(
                        "INSERT INTO harvest_devices (harvest_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
                        (harvest_id, portable_device_community_id, insert_quantity),
                    )

    def calculate_yields(self):
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
                    current_season = get_season(month)
                    if current_season == "Summer":
                        precipitation_types = ["rain", "without_prec"]
                        weights = [0.3, 0.7]  # Adjust the weights according to your preference
                        temp_range = (15, 30)
                        humidity_range = (30, 80)
                    elif current_season == "Winter":
                        precipitation_types = ["snow", "hail", "without_prec"]
                        weights = [0.2, 0.1, 0.7]  # Adjust the weights according to your preference
                        temp_range = (-10, 5)
                        humidity_range = (20, 70)
                    elif current_season == "Spring" or month == 9 or month == 10:
                        precipitation_types = ["rain", "hail", "without_prec"]
                        weights = [0.4, 0.01, 0.59]  # Adjust the weights according to your preference
                        temp_range = (10, 20)
                        humidity_range = (20, 60)
                    else:
                        precipitation_types = ["rain", "snow", "hail", "without_prec"]
                        weights = [0.20, 0.30, 0.1, 0.4]  # Adjust the weights according to your preference
                        temp_range = (0, 15)
                        humidity_range = (40, 70)

                    prec_type = random.choices(precipitation_types, weights=weights)[0]
                    humidity = random.randint(*humidity_range)
                    temperature = random.randint(*temp_range)
                    if prec_type == "rain":
                        rain_drop = random.randint(10, 100)
                    else:
                        rain_drop = None
                    if prec_type is not None:
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

    def insert_product_types(self):
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

    def insert_revenues(self):
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
        precipitation_types = [("rain",), ("snow",), ("hail",), ("without_prec",)]

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
        for category, devices in portable_device_data.items():
            for device_name in devices:
                self.cursor_obj.execute(
                    "INSERT INTO portable_devices (name) VALUES (%s)", (device_name,)
                )


    def insert_cultivations(self):
        self.cursor_obj.execute(
            """
            SELECT p.date, h.date, p.record_id 
            FROM plantings p 
            INNER JOIN harvests h ON p.record_id = h.record_id
        """
        )
        sowing_harvest_record_dates = self.cursor_obj.fetchall()
        for sowing_date, harvest_date, record_id in sowing_harvest_record_dates:
            start_date = (
                sowing_date + datetime.timedelta(days=random.randint(1, 2))
            ).strftime("%Y-%m-%d %H:%M:%S.%f %z")
            end_date = (
                harvest_date - datetime.timedelta(days=random.randint(1, 2))
            ).strftime("%Y-%m-%d %H:%M:%S.%f %z")
            self.cursor_obj.execute(
                """
                SELECT community_id 
                FROM records 
                WHERE id = %s
            """,
                (record_id,),
            )
            community_id = self.cursor_obj.fetchone()[0]
            query = f"""
                SELECT AVG(humidity), AVG(temperature) 
                FROM weather_metrics 
                WHERE community_id = {community_id} AND date BETWEEN '{start_date}' AND '{end_date}'
            """
            self.cursor_obj.execute(query)
            avg_humidity, avg_temp = self.cursor_obj.fetchone()
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

            avg_humidity = round(avg_humidity, 2)
            avg_temp = round(avg_temp, 2)
            if(water_amount is not None):
                water_amount = round(water_amount, 2)
            else:
                water_amount = 0

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

            other_factors = round(random.uniform(0.1, 0.49), 2)
            for product in products_armenia:
                if product[0] == product_name:
                    min_count_fertilizer = product[12]
                    max_count_fertilizer = product[13]
                    fertilizer_quantity = round(
                        random.randint(min_count_fertilizer, max_count_fertilizer)
                    )
                    break
            irrigation_hours = random.randint(18, 25)
            fertilizing_hours = random.randint(18, 25)
            soil_compaction_hours = random.randint(18, 25)

            self.cursor_obj.execute(
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

    def insert_plantings(self, duration):
        # Fetching necessary data
        self.cursor_obj.execute("""
            SELECT r.id, r.product_id, r.field_id, f.size
            FROM records r
            JOIN fields f ON r.field_id = f.id
        """)
        records_data = self.cursor_obj.fetchall()

        used_records = {}

        for record_id, product_id, field_id, field_size in records_data:
            if field_id in used_records:
                if len(used_records[field_id]) >= duration:
                    continue
                used_years = used_records[field_id]
            else:
                used_years = []
            available_years = [year for year in range(1, duration + 1) if year not in used_years]
            rand_year = random.choice(available_years)
            used_records.setdefault(field_id, []).append(rand_year)

            # Fetch product name and crop info
            self.cursor_obj.execute("SELECT name FROM products WHERE id = %s", (product_id,))
            product_name = self.cursor_obj.fetchone()[0]

            crop_info = self.get_crops_and_months_by_product_name(products_armenia, product_name)

            if crop_info is None:
                continue
            
            min_crop_count, max_crop_count, min_month, max_month = crop_info
            
            # Generate random date within specified months
            random_date_generated = random_date_within_months(min_month, max_month, rand_year)

            # Calculate workers count and crop count
            workers_count = min(max(field_size // 10 + random.randint(-3, 2), 0), 9)
            crop_count = field_size * random.randint(min_crop_count, max_crop_count) / 1000

            # Insert planting record
            self.cursor_obj.execute("""
                INSERT INTO plantings (record_id, crop_quantity, date, workers_quantity)
                VALUES (%s, %s, %s, %s)
            """, (record_id, crop_count, random_date_generated, workers_count))

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
                            (
                                min_growth_duration,
                                max_growth_duration,
                                min_yield,
                                max_yield,
                            ) = get_growth_duration_and_min_max_yield_by_product_name(
                                products_armenia, product_name
                            )
                            rand_day = random.randint(
                                min_growth_duration, max_growth_duration
                            )
                            harvest_date_day = planting_date.day + rand_day
                            planting_date_month = planting_date.month
                            harvest_year = planting_date.year
                            harvest_month = planting_date.month
                            if harvest_date_day >= 28:
                                harvest_day = harvest_date_day % 28
                                harvest_month = (
                                    harvest_date_day // 28 + planting_date_month
                                )
                            if (planting_date_month + harvest_month) >= 12:
                                harvest_year += 1
                                harvest_month = (
                                    planting_date_month + harvest_month
                                ) % 12
                            if harvest_day == 0:
                                harvest_day += 1
                            if harvest_month == 0:
                                harvest_month += 1
                            harvest_date = datetime.datetime(
                                harvest_year, harvest_month, harvest_day
                            )
                            field_id = record[2]
                            self.cursor_obj.execute(
                                f"SELECT size FROM fields WHERE id = {field_id}"
                            )
                            field_size = self.cursor_obj.fetchone()[0]
                            workers_count = field_size // 10
                            if workers_count > 9:
                                workers_count -= random.randint(0, 2)
                            else:
                                workers_count += random.randint(0, 2)
                            product_yield = field_size * random.randint(
                                min_yield, max_yield
                            )
                            planting_date = planting_date.strftime(
                                "%Y-%m-%d %H:%M:%S.%f %z"
                            )
                            harvest_date = harvest_date.strftime(
                                "%Y-%m-%d %H:%M:%S.%f %z"
                            )
                            self.cursor_obj.execute(
                                "INSERT INTO harvests (record_id, yield, date, acres_cut, workers_quantity) VALUES (%s, %s, %s, %s, %s)",
                                (
                                    record_id,
                                    product_yield,
                                    harvest_date,
                                    field_size,
                                    workers_count,
                                ),
                            )


    def insert_cultivation_devices(self):
        # Fetch all cultivations and portable devices communities
        self.cursor_obj.execute("SELECT id FROM cultivations")
        cultivations = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, quantity FROM portable_devices_communities")
        portable_devices_communities = self.cursor_obj.fetchall()

        for cultivation_id, in cultivations:
            for portable_device_community_id, portable_device_community_quantity in portable_devices_communities:
                insert_quantity = calculate_insert_quantity(portable_device_community_quantity)
                if insert_quantity > 0:
                    self.cursor_obj.execute(
                        "INSERT INTO cultivation_devices (cultivation_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
                        (cultivation_id, portable_device_community_id, insert_quantity),
                    )

    def insert_devices_calendars(self):
        self.cursor_obj.execute("SELECT id, field_id FROM records")
        records = self.cursor_obj.fetchall()
        self.cursor_obj.execute(
            "SELECT record_id, start_date, end_date, id FROM cultivations"
        )
        cultivations = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT record_id, date, id FROM harvests")
        harvests = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT record_id, date, id FROM plantings")
        plantings = self.cursor_obj.fetchall()
        for planting in plantings:
            planting_record_id = planting[0]
            planting_start_date = planting[1]
            planting_id = planting[2]
            for record in records:
                record_id = record[0]
                if record_id == planting_record_id:
                    record_field_id = record[1]
                    self.cursor_obj.execute(
                        f"SELECT portable_devices_communities_id, quantity FROM planting_devices WHERE planting_id = {planting_id}"
                    )
                    planting_devices = self.cursor_obj.fetchall()
                    for cultivation in cultivations:
                        cultivation_record_id = cultivation[0]
                        if cultivation_record_id == record_id:
                            cultivation_start_date = cultivation[1]
                            planning_end_date = (
                                cultivation_start_date
                                + datetime.timedelta(days=random.randint(-2, 2))
                            )
                            for planting_device in planting_devices:
                                portable_device_community_id = planting_device[0]
                                portable_device_quantity = planting_device[1]
                                self.cursor_obj.execute(
                                    "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (
                                        planting_start_date,
                                        planning_end_date,
                                        cultivation_start_date,
                                        portable_device_community_id,
                                        record_field_id,
                                        portable_device_quantity,
                                    ),
                                )
        for cultivation in cultivations:
            cultivation_record_id = cultivation[0]
            cultivation_start_date = cultivation[1]
            cultivation_end_date = cultivation[2]
            cultivation_id = cultivation[3]
            for record in records:
                record_id = record[0]
                if record_id == cultivation_record_id:
                    record_field_id = record[1]
                    self.cursor_obj.execute(
                        f"SELECT portable_devices_communities_id, quantity FROM cultivation_devices WHERE cultivation_id = {cultivation_id}"
                    )
                    cultivation_devices = self.cursor_obj.fetchall()
                    planning_end_date = cultivation_end_date + datetime.timedelta(
                        days=random.randint(-2, 2)
                    )
                    for cultivation_device in cultivation_devices:
                        portable_devices_communities_id = cultivation_device[0]
                        portable_device_quantity = cultivation_device[1]
                        self.cursor_obj.execute(
                            "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
                            (
                                cultivation_start_date,
                                planning_end_date,
                                cultivation_end_date,
                                portable_devices_communities_id,
                                record_field_id,
                                portable_device_quantity,
                            ),
                        )

        for harvest in harvests:
            harvest_date = harvest[1]
            harvest_record_id = harvest[0]
            harvest_id = harvest[2]
            for record in records:
                record_id = record[0]
                if record_id == harvest_record_id:
                    record_field_id = record[1]
                    self.cursor_obj.execute(
                        f"SELECT portable_devices_communities_id, quantity FROM harvest_devices WHERE harvest_id = {harvest_id}"
                    )
                    harvest_devices = self.cursor_obj.fetchall()
                    for cultivation in cultivations:
                        cultivation_record_id = cultivation[0]
                        if cultivation_record_id == record_id:
                            cultivation_end_date = cultivation[2]
                            planning_end_date = harvest_date + datetime.timedelta(
                                days=random.randint(-2, 2)
                            )
                            for harvest_device in harvest_devices:
                                portable_device_communitie_id = harvest_device[0]
                                portable_device_quantity = harvest_device[1]
                                self.cursor_obj.execute(
                                    "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (
                                        cultivation_end_date,
                                        planning_end_date,
                                        harvest_date,
                                        portable_device_communitie_id,
                                        record_field_id,
                                        portable_device_quantity,
                                    ),
                                )
        con.commit()
        self.cursor_obj.close()
        con.close()