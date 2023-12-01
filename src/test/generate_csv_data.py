import csv
import os
import random
from datetime import datetime, timedelta

customer_ids = list(range(1, 100))
store_ids = list(range(571, 576))
product_data = {
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
sales_persons = {
    571: [1, 2, 3],
    572: [4, 5, 6],
    573: [7, 8, 9],
    574: [10, 11, 12],
    575: [13, 14, 15],
}

start_date = datetime(2021, 2, 10)
end_date = datetime(2023, 10, 22)

file_location = "D:\\Projects\\PycharmProjects\\Spark_project_1\\data\\sales_data_to_s3"
csv_file_path = os.path.join(file_location, "sales_data5.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(
        [
            "customer_id",
            "store_id",
            "product_name",
            "sales_date",
            "sales_person_id",
            "price",
            "quantity",
            "total_cost",
        ]
    )

    for _ in range(1996985):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days)
        )
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity

        csvwriter.writerow(
            [
                customer_id,
                store_id,
                product_name,
                sales_date.strftime("%Y-%m-%d"),
                sales_person_id,
                price,
                quantity,
                total_cost,
            ]
        )

print("CSV file generated successfully.")
