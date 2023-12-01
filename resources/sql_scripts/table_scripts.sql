CREATE TABLE product_staging_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    file_location VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(1)
);


CREATE TABLE customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(255),
    pincode VARCHAR(10),
    phone_number VARCHAR(20),
    customer_joining_date DATE
);

-- Insert command for customer
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Nirvi', 'D’Alia', 'Pune', '411012', '+91-7856831395', '2021-12-12');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Onkar', 'Sachdeva', 'Pune', '411003', '+91-7836947872', '2022-12-08');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Manikya', 'Lalla', 'Pune', '411012', '+91-9840753318', '2022-08-16');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Saanvi', 'Sarraf', 'Pune', '411005', '+91-9861232318', '2023-01-01');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Shaan', 'Dube', 'Pune', '411005', '+91-9745992577', '2023-07-13');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Anaya', 'Gara', 'Pune', '411012', '+91-7918665240', '2021-09-24');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Dhanush', 'Choudhry', 'Pune', '411005', '+91-9958208679', '2022-04-02');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Samar', 'Dani', 'Pune', '411031', '+91-7805311817', '2022-02-26');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Damini', 'Bava', 'Pune', '411003', '+91-7835155416', '2022-06-21');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Oorja', 'Vohra', 'Pune', '411012', '+91-9810880111', '2023-09-10');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Aayush', 'Lad', 'Pune', '411005', '+91-7947138269', '2022-12-14');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Keya', 'Seth', 'Pune', '411031', '+91-7956676633', '2022-09-20');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Emir', 'Gour', 'Pune', '411031', '+91-7812868739', '2022-11-09');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Anahita', 'Sethi', 'Pune', '411012', '+91-9918212398', '2023-06-12');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Dishani', 'Deol', 'Pune', '411031', '+91-8956622699', '2021-03-15');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Kimaya', 'Seth', 'Pune', '411031', '+91-7831223547', '2021-09-09');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Anika', 'Dhaliwal', 'Pune', '411003', '+91-7737509958', '2021-01-24');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Mishti', 'Karan', 'Pune', '411031', '+91-9745235535', '2022-01-18');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Aayush', 'Kashyap', 'Pune', '411003', '+91-7817954318', '2022-08-22');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Amani', 'Rout', 'Pune', '411003', '+91-7871708114', '2022-02-11');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Myra', 'Khalsa', 'Pune', '411003', '+91-8806066277', '2021-04-25');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Devansh', 'Mangal', 'Pune', '411031', '+91-9854813907', '2022-06-23');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Taimur', 'Dey', 'Pune', '411031', '+91-9936992797', '2022-07-04');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Taran', 'Lala', 'Pune', '411003', '+91-9847852610', '2021-11-30');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Tiya', 'Anand', 'Pune', '411003', '+91-8811856481', '2022-09-29');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Kiaan', 'Chaudhry', 'Pune', '411031', '+91-8865104671', '2023-06-08');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Jivin', 'Ram', 'Pune', '411003', '+91-9745468577', '2021-07-12');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Aniruddh', 'Bhatti', 'Pune', '411012', '+91-7877574778', '2022-05-15');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Chirag', 'Saraf', 'Pune', '411031', '+91-8792602494', '2023-05-28');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Abram', 'Dugar', 'Pune', '411003', '+91-7869815601', '2022-05-26');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Damini', 'Bala', 'Pune', '411012', '+91-7888870478', '2021-07-06');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Veer', 'Mammen', 'Pune', '411005', '+91-7709199512', '2022-02-19');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Pranay', 'Atwal', 'Pune', '411003', '+91-8779597420', '2023-08-14');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Diya', 'Chakrabarti', 'Pune', '411031', '+91-9764801524', '2023-09-11');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Rania', 'Wable', 'Pune', '411005', '+91-7710688343', '2021-08-03');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Saksham', 'Khurana', 'Pune', '411031', '+91-8733014522', '2023-09-17');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Mehul', 'Sunder', 'Pune', '411012', '+91-7774205085', '2022-08-26');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Ahana ', 'Bhalla', 'Pune', '411031', '+91-7757227544', '2021-06-11');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Tejas', 'Goda', 'Pune', '411003', '+91-8997989954', '2022-09-28');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Sara', 'Wable', 'Pune', '411003', '+91-7720969501', '2022-03-29');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Kabir', 'Talwar', 'Pune', '411012', '+91-9731671819', '2021-05-07');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Urvi', 'Sachdev', 'Pune', '411005', '+91-7835808106', '2023-01-29');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Jayant', 'Ramanathan', 'Pune', '411031', '+91-9705723780', '2021-03-25');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Amani', 'Bhatti', 'Pune', '411005', '+91-9839816591', '2022-08-28');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Anvi', 'Sandhu', 'Pune', '411005', '+91-8802386168', '2023-05-27');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Aayush', 'Johal', 'Pune', '411031', '+91-7787425099', '2023-01-13');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Parinaaz', 'Gola', 'Pune', '411012', '+91-7907271130', '2022-07-27');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Keya', 'Anand', 'Pune', '411012', '+91-7916958760', '2022-04-20');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Biju', 'Gera', 'Pune', '411031', '+91-9845861003', '2022-06-03');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Ela', 'Rau', 'Pune', '411012', '+91-9856695504', '2021-08-26');

--store table
CREATE TABLE store (
    id INT PRIMARY KEY,
    address VARCHAR(255),
    store_pincode VARCHAR(10),
    store_manager_name VARCHAR(100),
    store_opening_date DATE,
    reviews TEXT
);

--data of store table
INSERT INTO store (id, address, store_pincode, store_manager_name, store_opening_date, reviews)
VALUES
    (571,'Pune', '411002', 'Manish', '2020-01-15', 'Great store with a friendly staff.'),
    (572,'Pune', '411007', 'Nikita', '2020-08-10', 'Excellent selection of products.'),
    (573,'Pune', '411014', 'Vikash', '2021-01-20', 'Clean and organized store.'),
    (574,'Pune', '411020', 'Rakesh', '2020-05-05', 'Good prices and helpful staff.'),
    (575,'Pune', '411023', 'Harshit', '2020-12-19', 'Great range of products available.');



-- product table
CREATE TABLE product (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    current_price DECIMAL(10, 2),
    old_price DECIMAL(10, 2),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    expiry_date DATE
);


--product table data
--INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date)
--VALUES
--    ('quaker oats', 212, 212, '2022-05-15', NULL, '2025-01-01');
INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date)
VALUES
    ('quaker oats', 212, 190, '2019-10-27', NULL, '2025-07-15'),
    ('sugar', 50, 35, '2020-02-27', NULL, '2024-10-18'),
    ('maida', 20, 8, '2019-10-23', NULL, '2025-03-17'),
    ('besan', 52, 46, '2020-02-27', NULL, '2025-03-24'),
    ('mustard oil', 110, 88, '2020-06-20', NULL, '2024-04-13'),
    ('clinic plus', 345, 276, '2019-09-27', NULL, '2024-08-24'),
    ('dantkanti', 100, 80, '2020-06-12', NULL, '2024-12-08'),
    ('nutrella', 40, 36, '2020-05-09', NULL, '2024-08-20'),
    ('sunflower oil', 120, 84, '2019-08-11', NULL, '2025-04-03'),
    ('cashews', 340, 237, '2019-08-17', NULL, '2025-06-07'),
    ('atta', 450, 360, '2019-07-13', NULL, '2024-08-08'),
    ('kabuli chana', 140, 98, '2019-08-18', NULL, '2024-12-12'),
    ('coriander powder', 50, 32, '2019-09-22', NULL, '2024-10-31'),
    ('amul ghee', 596, 566, '2020-05-19', NULL, '2024-12-29'),
    ('dove shampoo', 574, 401, '2020-02-07', NULL, '2025-07-07'),
    ('black pepper', 52, 20, '2019-09-21', NULL, '2024-06-21'),
    ('maggi masala', 80, 64, '2020-03-26', NULL, '2025-06-23'),
    ('hair gel', 65, 58, '2019-10-15', NULL, '2024-10-10'),
    ('deodorant', 141, 98, '2019-08-08', NULL, '2024-06-08');

--sales team table
CREATE TABLE sales_team (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    manager_id INT,
    is_manager CHAR(1),
    address VARCHAR(255),
    pincode VARCHAR(10),
    joining_date DATE
);


--sales team data
INSERT INTO sales_team (first_name, last_name, manager_id, is_manager, address, pincode, joining_date)
VALUES
    ('Siya', 'Buch', 3, 'N', 'Pune', '411022', '2023-06-30'),
    ('Reyansh', 'Ray', 3, 'N', 'Pune', '411037', '2023-02-07'),
    ('Bhamini', 'Sama', 2, 'N', 'Pune', '411015', '2021-05-27'),
    ('Badal', 'Saraf', 3, 'N', 'Pune', '411022', '2021-03-05'),
    ('Divij', 'Bajaj', 2, 'N', 'Pune', '411037', '2022-10-04'),
    ('Mamooty', 'Kanda', 3, 'N', 'Pune', '411037', '2023-04-27'),
    ('Anika', 'De', 3, 'N', 'Pune', '411037', '2022-12-12'),
    ('Mehul', 'Borah', 2, 'N', 'Pune', '411037', '2022-05-16'),
    ('Miraan', 'Savant', 3, 'N', 'Pune', '411037', '2022-01-06'),
    ('Sahil', 'Soni', 1, 'N', 'Pune', '411006', '2021-07-25'),
    ('Yakshit', 'Goswami', 1, 'N', 'Pune', '411037', '2022-11-03'),
    ('Vardaniya', 'Hayer', 3, 'N', 'Pune', '411015', '2022-02-14'),
    ('Indranil', 'Mand', 2, 'N', 'Pune', '411015', '2023-09-20'),
    ('Amira', 'Bal', 4, 'N', 'Pune', '411037', '2022-07-11'),
    ('Zoya', 'Sarkar', 1, 'N', 'Pune', '411037', '2023-07-31');





--s3 bucket table
CREATE TABLE s3_bucket_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bucket_name VARCHAR(255),
    file_location VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(20)
);


--s3 bucket data
INSERT INTO s3_bucket_info (bucket_name, status, created_date, updated_date)
VALUES ('pyspark.project', 'active', NOW(), NOW());


--Data Mart customer
CREATE TABLE customers_data_mart (
    customer_id INT ,
    full_name VARCHAR(100),
    address VARCHAR(200),
    phone_number VARCHAR(20),
    purchase_month VARCHAR(20),
    total_sales DECIMAL(10, 2)
);


--sales mart table
CREATE TABLE sales_team_data_mart (
    store_id INT,
    sales_person_id INT,
    full_name VARCHAR(255),
    sales_month VARCHAR(10),
    total_sales DECIMAL(10, 2),
    incentive DECIMAL(10, 2)
);