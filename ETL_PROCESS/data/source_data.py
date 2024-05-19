import psycopg2
import random


# Параметры подключения к базе данных
connection_params = {
    'dbname': 'data_source',
    'user': 'postgres',
    'password': 'pass',
    'host': 'localhost',
    'port': '5432'
}

# SQL-команда для создания таблицы
create_table_query = '''
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL,
    quantity INT,
    category VARCHAR(50)
);
'''

# SQL-команда для вставки данных
insert_query = '''
INSERT INTO products (name, price, quantity, category) VALUES
''' + ", ".join([f"('Product {i}', {random.randint(10, 100)}, {random.randint(1, 20)}, '{random.choice(['Electronics', 'Clothing', 'Toys', 'Food', 'Furniture'])}')" for i in range(1, 101)]) + ";"

try:
    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()

    # Создание таблицы
    cur.execute(create_table_query)
    conn.commit()

    # Вставка данных
    cur.execute(insert_query)
    conn.commit()

    print('Таблица создана и данные вставлены успешно.')
except Exception as e:
    print(f"Произошла ошибка: {e}")
finally:
    # Закрытие подключения
    if conn:
        cur.close()
        conn.close()
