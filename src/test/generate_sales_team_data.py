import random
from faker import Faker
from datetime import datetime, timedelta, date

fake = Faker("en_IN")
nums = [1, 2, 3, 4]
first_name = fake.first_name()
last_name = fake.last_name()
manager_id = random.choice(nums)
is_manager = "N"
address = "Pune"
joining_date = fake.date_between_dates(
    date_start=datetime(2021, 1, 23), date_end=datetime(2023, 9, 25)
).strftime("%Y-%m-%d")
pincode_list = [411006, 411015, 411022, 411037]


for i in range(15):
    first_name = fake.first_name()
    last_name = fake.last_name()
    manager_id = random.choice(nums)
    is_manager = "N"
    address = "Pune"
    joining_date = fake.date_between_dates(
        date_start=datetime(2021, 1, 23), date_end=datetime(2023, 9, 25)
    ).strftime("%Y-%m-%d")
    pincode = random.choice(pincode_list)
    print(
        f"('{first_name}', '{last_name}', {manager_id}, '{is_manager}', '{address}', '{pincode}', '{joining_date}'),"
    )
