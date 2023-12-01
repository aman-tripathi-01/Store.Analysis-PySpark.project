import random
from faker import Faker
from datetime import datetime, timedelta, date

fake = Faker("en_IN")
products = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "mustard oil": 110,
    "clinic plus": 345,
    "dantkanti": 100,
    "nutrella": 40,
    "sunflower oil": 120,
    "cashews": 340,
    "atta": 450,
    "kabuli chana": 140,
    "coriander powder": 50,
    "amul ghee": 596,
    "dove shampoo": 574,
    "black pepper": 52,
    "maggi masala": 80,
    "hair gel": 65,
    "deodorant": 141,
}
old_price_range = [10, 20, 30, 5, 15, 35, 27, 60]
product_list = list(products.keys())

created_date_1, created_date_2 = date(2019, 6, 3), date(2020, 7, 1)
dates_bet = created_date_2 - created_date_1
total_days = dates_bet.days

expiry_date_1, expiry_date_2 = date(2024, 3, 7), date(2025, 9, 30)
dates_bet_2 = expiry_date_2 - expiry_date_1
total_days_2 = dates_bet_2.days

for i in range(len(products)):
    name = product_list[i]
    current_price = products[name]
    old_price = int(current_price * ((100 - random.choice(old_price_range)) / 100))
    created_date = created_date_1 + timedelta(random.randrange(1, total_days))
    updated_date = "NULL"
    expiry_date = expiry_date_1 + timedelta(random.randrange(1, total_days_2))
    print(
        f"('{name}', {current_price}, {old_price}, '{created_date}', '{updated_date}', '{expiry_date}'),"
    )
