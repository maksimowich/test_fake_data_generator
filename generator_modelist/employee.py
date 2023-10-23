# 1. Предложение основное:
#     • - id предложения
#     • - id клиента
#     • - дата начала действия предложения
#     • - дата окончания действия предложения
#     • - актуальный статус предложения
#     • - продукт
#     • - код кампании
#     • - сегмент
#     • - дата изменения записи
#     • - комментарий

from fake_data_generator import generate_table_from_profile, \
    Column, MultipleColumns, ContinuousColumn, StringFromRegexColumn, CategoricalColumn, IncrementalIDColumn, ForeignKeyColumn
from sqlalchemy import create_engine
import pandas as pd
import random
import sqlalchemy
from pandas import Series, DataFrame
from datetime import datetime

db_url = "postgresql://postgres:12345@egorbugakov.ru:5432/postgres"
engine = create_engine(db_url)

table_name = 'generator_modelist.test_table1'


def get_generator_multiple_columns(conn, table_name, incremental_id_column_name):
    output_size = yield
    start = 1262293200 # 2010-01-01 00:00:00
    end = 1577826000 # 2020-01-01 00:00:00

    current_max_id_df = pd.read_sql_query(f'SELECT MAX({incremental_id_column_name}) AS id FROM {table_name}', conn)
    current_id = current_max_id_df['id'][0] + 1 if current_max_id_df['id'][0] is not None else 1

    while True:
        dttm_and_id = []
        number_of_left_rows = output_size
        while number_of_left_rows > 0:
            random_number = min(random.choice([1, 2, 3, 4]), number_of_left_rows)
            number_of_left_rows = number_of_left_rows - random_number
            dttm = random.randint(start, end)
            for _ in range(random_number):
                dttm_and_id.append([datetime.fromtimestamp(dttm), current_id])
                dttm += random.randint(0, 31553280)
            current_id += 1
        output_size = yield DataFrame(dttm_and_id)


dttm_and_id_columns = MultipleColumns(
    columns=[
        Column(column_name='dttm', data_type='timestamp'),
        Column(column_name='id', data_type='int'),
    ],
    generator=get_generator_multiple_columns(engine, table_name, 'id')
)

generate_table_from_profile(conn=engine,
                            dest_table_name_with_schema=table_name,
                            number_of_rows_to_insert=100,
                            # source_table_profile_path=f'{table_name}.json',
                            columns_info=[
                                dttm_and_id_columns
                            ],
                            batch_size=20)

df = pd.read_sql_query(sql=f'select * from {table_name} order by id desc limit 40', con=engine)
print(df.to_string())
print(pd.read_sql_query(sql=f'select count(*) from {table_name}', con=engine))
