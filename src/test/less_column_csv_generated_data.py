import os
import csv
import random
from datetime import datetime

customer_ids = list(range(1, 100))
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

file_location = "D:\\Projects\\PycharmProjects\\Spark_project_1\\data\\sales_data_to_s3"

if not os.path.exists(file_location):
    os.makedirs(file_location)

input_date_str = input("Enter the date for which you want to generate (YYYY-MM-DD): ")
input_date = datetime.strptime(input_date_str, "%Y-%m-%d")

csv_file_path = os.path.join(file_location, f"less_c2olumn_sales_data_{input_date_str}.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(
        [
            "customer_id",
            "product_name",
            "sales_date",
            "sales_person_id",
            "price",
            "quantity",
            "total_cost",
            "payment_mode",
        ]
    )

    for _ in range(400000):
        customer_id = random.choice(customer_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = input_date
        sales_person_id = random.choice(list(sales_persons.values()))
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity
        payment_mode = random.choice(["Cash", "UPI", "Store credit"])

        csvwriter.writerow(
            [
                customer_id,
                product_name,
                sales_date.strftime("%Y-%m-%d"),
                sales_person_id,
                price,
                quantity,
                total_cost,
                payment_mode,
            ]
        )

    print("CSV file generated successfully:", csv_file_path)
