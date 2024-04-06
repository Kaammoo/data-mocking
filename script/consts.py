max_field_count = 30
min_field_count = 15
max_field_size = 150
min_field_size = 30
duration = 5 
min_users_per_community1 = 15
max_users_per_community2 = 20
community_weights = {
            "Vanadzor": 0.9,
            "Alaverdi": 0.8,
            "Spitak": 0.7,
            "Stepanavan": 0.5,
            "Tashir": 0.3,
            "Akhtala": 0.4,
            "Tumanyan": 0.5,
            "Gyulagarak": 0.3,
            "Pambak": 0.6,
            "Odzun": 0.7
        }

portable_device_data = (
    {"truck":("BelAZ 75710", "KAMAZ-65225", "MAZ-6501", "KrAZ-7140H6", "URAL-5557",
    "BelAZ 75302", "KAMAZ-65228", "MAZ-6516", "KrAZ-65101", "URAL-4320",
    "BelAZ 7555", "KAMAZ-65801", "MAZ-6517", "KrAZ-65053", "URAL-55571",
    "BelAZ 75131", "KAMAZ-65115", "MAZ-65115", "KrAZ-6322", "URAL-55572",
    "BelAZ 7557", "KAMAZ-6520", "MAZ-5551", "KrAZ-65055", "URAL-43206",
    "BelAZ 75135", "KAMAZ-65802", "MAZ-6515", "KrAZ-65032", "URAL-55573",
    "ZIL 130", "ZIL 4331", "ZIL 164"), 
    "instrument":("Shovel", "Spade", "Rake", "Hoe"),
    "tractor":("Tractor Belarus 820", "Tractor MTZ-1221", "Tractor Kirovets K-744", 
    "Tractor T-150K", "Tractor DT-75", "Tractor YUMZ-6", "Tractor KhTZ-181",
    "Tractor T-25", "Tractor T-40", "Tractor LTZ T-170", "Tractor DT-75M",
    "Tractor YUMZ-6K", "Tractor Belarus 1025", "Tractor MTZ-80", "Tractor T-16",
    "Tractor K-700A", "Tractor Belarus 892", "Tractor T-150", "Tractor MTZ-52","Tractor T-70"),
    "combine":("Combine Bizon Z056", "Combine Niva SK-5",
    "Combine Yenisei-1200", "Combine Don-1500", "Combine PALESSE GS12",
    "Combine Slobozhanets-20", "Combine Yenisei-1200A", "Combine PALESSE GS16",
    "Combine Dnieper-7", "Combine Niva SK-6", "Combine PALESSE GS575",
    "Combine Yenisei-1200M", "Combine PALESSE GS12R", "Combine Niva SK-3",
    "Combine Don-1500B", "Combine Yenisei-1200N", "Combine PALESSE GS4118",
    "Combine Niva SK-5M", "Combine Don-1500E", "Combine PALESSE GS4118K",)}
)
# products_armenia[0] -> Product Name
# products_armenia[1] -> Product Description
# products_armenia[2] -> Product Min Sown count (grams) for  hectar
# products_armenia[3] -> Product Max Sown count (grams) for  hectar
# products_armenia[4] -> Month on which  this product is sown
# products_armenia[5] -> From which to Month on which  this product is sown
# products_armenia[6] -> Product Name
products_armenia = (
    ("Russet Potato", "Versatile and delicious vegetable.", 1500, 1800, 5, 5, 85, 95, 8, 25, 15000, 40000, 50, 100),
    ("Sweet Potato", "Versatile and delicious vegetable.", 1500, 1800, 5, 5, 85, 95, 8, 25, 10000, 30000, 50, 100),
    ("Parnik Potato", "Versatile and delicious vegetable.", 1500, 1800, 5, 5, 85, 95, 8, 25, 20000, 40000, 50, 100),
    ("Yukon Gold Potato", "Versatile and delicious vegetable.", 1500, 1800, 5, 5, 85, 95, 8, 25, 20000, 30000, 50, 100),
    ("Slicing Cucumber", "Crisp and refreshing vegetable.", 5000, 8000, 4, 5, 50, 70, 15, 25, 20000, 30000, 100, 150),
    ("Pickling Cucumber", "Crisp and refreshing vegetable.", 5000, 8000, 4, 5, 50, 70, 15, 25, 20000, 30000, 100, 150),
    ("Persian Cucumber", "Crisp and refreshing vegetable.", 5000, 8000, 4, 5, 50, 70, 15, 25, 20000, 30000, 100, 150),
    ("Tomato", "Juicy and flavorful vegetable.", 300, 1000, 4, 5, 69, 93, 18, 25, 10000, 60000, 80, 120),
    ("Wheat", "Nutritious grain for various culinary uses cereal.", 1000, 3000, 3, 5, 90, 150, 15, 25, 2000, 4000, 50, 80),
    ("Buckwheat", "Nutritious grain for various culinary uses cereal.", 1000, 3000, 3, 5, 90, 150, 15, 25, 2000, 4000, 50, 80),
    ("Carrot", "Sweet and crunchy vegetable.", 5000, 6000, 3, 4, 74, 105, 6, 17, 20000, 60000, 100, 150),
    ("Eggplant", "Rich and savory vegetable.", 300, 500, 4, 5, 73, 99, 14, 24, 5000, 8000, 50, 100),
    ("Squash", "Delicious in soups, stews, and casseroles vegetable.", 200, 400, 5, 6, 35, 40, 10, 12, 30000, 40000, 80, 120),
    ("Sugar Pie Pumpkin", "Perfect for pies and soups vegetable.", 2500, 4000, 5, 6, 110, 120, 10, 12, 40000, 80000, 100, 150),
    ("Cinderella Pumpkin", "Perfect for pies and soups vegetable.", 2500, 4000, 5, 6, 110, 120, 10, 12, 40000, 80000, 100, 150),
    ("Butternut Pumpkin", "Perfect for pies and soups vegetable.", 2500, 4000, 5, 6, 110, 120, 10, 12, 40000, 80000, 100, 150),
    ("Buckwheat", "Gluten-free grain with a nutty flavor. cereal", 1000, 3000, 3, 5, 90, 150, 15, 25, 2000, 4000, 50, 80),
    ("Red Potato", "Versatile and delicious vegetable.", 1500, 1800, 5, 5, 85, 95, 8, 25, 20000, 40000, 50, 100),
    ("Cabbage", "Versatile and delicious vegetable.", 2000, 3000, 4, 5, 123, 136, 3, 15, 20000, 50000, 100, 150),
    ("Green Cabbage", "Classic green cabbage variety vegetable.", 2000, 3000, 4, 5, 123, 136, 3, 15, 20000, 50000, 100, 150),
    ("Red Cabbage", "Vibrant purple-red cabbage variety vegetable.", 2000, 3000, 4, 5, 123, 136, 3, 15, 20000, 50000, 100, 150),
    ("Savoy Cabbage", "Crinkled and sweet-tasting cabbage variety vegetable.", 2000, 3000, 4, 5, 123, 136, 3, 15, 20000, 50000, 100, 150),
    ("Lettuce", "Crisp and tender lettuce leaves vegetable.", 5000, 6000, 4, 4, 74, 105, 6, 17, 15000, 30000, 100, 150),
    ("Spinach", "Nutrient-rich and versatile leafy green vegetable.", 3000, 4000, 8, 9, 35, 40, 2, 3, 1000, 1000, 50, 100),
    ("Radish", "Peppery and crisp root vegetable.", 1500, 2000, 9, 10, 20, 30, 1, 2, 10000, 15000, 50, 100),
    ("White Onion", "Pungent and flavorful bulb vegetable.", 10000, 12000, 3, 4, 115, 145, 2, 3, 40000, 47000, 100, 150),
    ("Sweet Onion", "Pungent and flavorful bulb vegetable.", 10000, 12000, 3, 4, 115, 145, 2, 3, 40000, 47000, 100, 150),
    ("Red Onion", "Pungent and flavorful bulb vegetable.", 10000, 12000, 3, 4, 115, 145, 2, 3, 40000, 47000, 100, 150),
    ("Garlic", "Aromatic and flavorful bulb vegetable.", 5000, 10000, 10, 11, 80, 140, 1, 2, 13000, 14000, 50, 100),
    ("Bell Red Pepper", "Sweet and colorful bell-shaped pepper vegetable.", 1500, 2000, 3, 4, 65, 80, 13, 17, 20000, 30000, 80, 120),
    ("Bell Green Pepper", "Sweet and colorful bell-shaped pepper vegetable.", 1500, 2000, 3, 4, 65, 80, 13, 17, 20000, 30000, 80, 120),
    ("Bell Yelow Pepper", "Sweet and colorful bell-shaped pepper vegetable.", 1500, 2000, 3, 4, 65, 80, 13, 17, 20000, 30000, 80, 120),
    ("Bell Orange Pepper", "Sweet and colorful bell-shaped pepper vegetable.", 1500, 2000, 3, 4, 65, 80, 13, 17, 20000, 30000, 80, 120),
    ("Sprouting Broccoli", "Nutrient-dense and versatile vegetable.", 2000, 2200, 3, 4, 70, 80, 2, 3, 12000, 18000, 80, 120),
    ("Calabrese Broccoli", "Nutrient-dense and versatile vegetable.", 2000, 2200, 3, 4, 70, 80, 2, 3, 12000, 18000, 80, 120),
    ("Purple Sprouting Broccoli", "Nutrient-dense and versatile vegetable.", 2000, 2200, 3 , 4, 70, 80, 2, 3, 12000, 18000, 80, 120),
    ("Cauliflower", "Mild-flavored and nutritious vegetable.", 300, 400, 3, 4, 55, 80, 2, 3, 12000, 18000, 80, 120),
    ("Green Bean", "Crunchy and tender green beans vegetable.", 7500, 8000, 4 , 5, 65, 120, 10, 12, 20000, 21000, 100, 150),
)    

