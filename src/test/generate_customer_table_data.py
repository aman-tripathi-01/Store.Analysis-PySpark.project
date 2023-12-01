import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")
pincode_list = [411003, 411005, 411012, 411031]

for _ in range(50):
    first_name = fake.first_name()
    last_name = fake.last_name()
    address = "Pune"
    pincode = random.choice(pincode_list)
    phone_number = (
        "+91-"
        + "".join([str(random.randint(7, 9)) for _ in range(2)])
        + "".join([str(random.randint(0, 9)) for _ in range(8)])
    )
    joining_date = fake.date_between_dates(
        date_start=datetime(2021, 1, 23), date_end=datetime(2023, 9, 25)
    ).strftime("%Y-%m-%d")

    print(
        f"INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('{first_name}', '{last_name}', '{address}', '{pincode}', '{phone_number}', '{joining_date}');"
    )
