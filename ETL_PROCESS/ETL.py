import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine

# Создание движка SQLAlchemy для подключения к PostgreSQL
engine = create_engine('postgresql://postgres:pass@localhost:5432/data_source')

# EXTRACT
def extract_json(file_path):
    return pd.read_json(file_path)

def extract_sql(engine, query):
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def extract_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = [{'id': int(product.find('id').text),
             'name': product.find('name').text,
             'price': float(product.find('price').text) if product.find('price').text else None,
             'quantity': int(product.find('quantity').text),
             'category': product.find('category').text}
            for product in root.findall('product')]
    return pd.DataFrame(data)

# TRANSFORM
def transform_data(df):
    """
    Функция transform_data принимает DataFrame и возвращает новый DataFrame после преобразования.
    """
    df = df.drop_duplicates(subset='id')
    df = df.fillna(value={'price': 0, 'quantity': 1})
    df['price'] = df['price'].astype(float)
    df['quantity'] = df['quantity'].astype(int)
    return df

# LOAD
def load_data(df, engine, table_name):
    """
     Функция load_data принимает DataFrame, движок базы данных и имя таблицы, в которую нужно загрузить данные.
    """
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# # Использование функций
# json_df = transform_data(extract_json('data/data.json'))
# sql_df = transform_data(extract_sql(engine, "SELECT id, name, price, quantity, category FROM public.products;"))
# xml_df = transform_data(extract_xml('data/data.xml'))

# # Объединение DataFrame
# combined_df = pd.concat([json_df, sql_df, xml_df], ignore_index=True)

# # Сортировка данных по цене в порядке возрастания и группировка по категориям
# sorted_df = combined_df.sort_values(by='price', ascending=True)
# grouped_df = sorted_df.groupby('category').agg({
#     'price': 'mean',
#     'quantity': 'sum'
# }).reset_index()

# # Загрузка данных в базу данных
# load_data(combined_df, engine, 'data_load')
# load_data(sorted_df, engine, 'data_sorted_load')
# load_data(grouped_df, engine, 'data_aggregated_load')
