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
from pandas import Series, DataFrame
from datetime import datetime

db_url = "postgresql://postgres:12345@egorbugakov.ru:5432/postgres"
engine = create_engine(db_url)

table_name = 'generator_modelist.offer'


id_col = IncrementalIDColumn(column_name='id', data_type='bigint')

client_id_col = ContinuousColumn(column_name='client_id', data_type='bigint', intervals=[(0, 10000)], probabilities=[1])


def get_generator_multiple_columns():
    start = 1262293200 # 2010-01-01 00:00:00
    end = 1577826000 # 2020-01-01 00:00:00
    output_size = yield
    while True:
        dates = []
        for _ in range(output_size):
            first_date = random.randint(start, end)
            second_date = first_date + random.randint(0, 1577826000 - 1262293200)
            dates.append([datetime.fromtimestamp(first_date), datetime.fromtimestamp(second_date)])
        output_size = yield DataFrame(dates)

start_dttm_and_end_dttm_columns = MultipleColumns(
    columns=[
        Column(column_name='start_dttm', data_type='timestamp'),
        Column(column_name='end_dttm', data_type='timestamp'),
    ],
    generator=get_generator_multiple_columns()
)

status_col = CategoricalColumn(column_name='status', data_type='varchar', values=['ACTIVE', 'NOT_ACTIVE'], probabilities=[0.2, 0.8])

product_col = CategoricalColumn(column_name='product', data_type='varchar', values=['PRODUCT_1', 'PRODUCT_2', 'PRODUCT_3'], probabilities=[0.2, 0.6, 0.2])

company_code_col = StringFromRegexColumn(column_name='company_code', data_type='varchar', common_regex='[A-Z][A-Z][A-Z]-[0-9][0-9]')

segment_col = CategoricalColumn(column_name='segment', data_type='varchar', values=['SEGMENT_1', 'SEGMENT_2', 'SEGMENT_3'], probabilities=[0.2, 0.6, 0.2])

changed_dttm_col = ContinuousColumn(column_name='changed_dttm', data_type='timestamp', intervals=[(1262293200, 1577826000)], probabilities=[1])

fk_col = ForeignKeyColumn(column_name='fk_col', data_type='int', foreign_key_table_name='adm.fk_table', foreign_key_column_name='id')

generate_table_from_profile(conn=engine,
                            dest_table_name_with_schema=f'{table_name}',
                            number_of_rows_to_insert=100,
                            # source_table_profile_path=f'{table_name}.json',
                            columns_info=[
                                id_col,
                                client_id_col,
                                start_dttm_and_end_dttm_columns,
                                status_col,
                                product_col,
                                company_code_col,
                                segment_col,
                                changed_dttm_col,
                                fk_col,
                            ],
                            batch_size=20)

df = pd.read_sql_query(sql=f'select * from {table_name} order by id desc limit 40', con=engine)
print(df.to_string())
print(pd.read_sql_query(sql=f'select count(*) from {table_name}', con=engine))