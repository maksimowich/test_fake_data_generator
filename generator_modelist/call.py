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


db_url = "postgresql://postgres:12345@egorbugakov.ru:5432/postgres"
engine = create_engine(db_url)

table_name = 'generator_modelist.test_table2'


fk1_col = ForeignKeyColumn(column_name='fk1',
                           data_type='int',
                           foreign_key_table_name='generator_modelist.test_table1',
                           foreign_key_column_name='id')


fk2_col = ForeignKeyColumn(column_name='fk2',
                           data_type='int',
                           foreign_key_table_name='generator_modelist.test_table1',
                           foreign_key_column_name='id')


generate_table_from_profile(conn=engine,
                            dest_table_name_with_schema=f'{table_name}',
                            number_of_rows_to_insert=100,
                            # source_table_profile_path=f'{table_name}.json',
                            columns_info=[
                                fk1_col,
                                fk2_col,
                            ],
                            batch_size=20)

df = pd.read_sql_query(sql=f'select * from {table_name} order by fk1 desc limit 40', con=engine)
print(df.to_string())
print(pd.read_sql_query(sql=f'select count(*) from {table_name}', con=engine))