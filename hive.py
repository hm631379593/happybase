import os
import pandas as pd
from impala.dbapi import connect


class Hive:
    def conn(self):
        return connect(host='192.168.1.66', port=10000, user='hive', password='sinocbd', database='default',
                       auth_mechanism='PLAIN')

    def create(self, name, df):
        hql = 'CREATE TABLE {}({})'
        df_keys = df.keys()
        column_name = ''
        for key in df_keys:
            column_name += '{} string, '.format(key.lower())
        return hql.format(name, column_name).replace(', )', ')')

    def destroy(self, name):
        return 'DROP TABLE {}'.format(name)

    def insert(self, table, df):
        hql = 'INSERT INTO {}({}) VALUES{}]'
        df_keys = df.keys()
        column_name = ''
        for key in df_keys:
            column_name += ' {}, '.format(key.lower())

        values = ''
        for index, row in df.iterrows():
            values += '('
            for key in df_keys:
                values += ' \'{}\', '.format(row[key])
            values += '), '

        hql = hql.format(table, column_name, values).replace(', )', ')').replace(', ]', '')
        return hql

    def get_name(self, file):
        return os.path.splitext(os.path.basename(file))[0].lower()

    def run(self, file):
        df = pd.read_csv(file)
        conn = self.conn()
        try:
            name = self.get_name(file)

            cursor = conn.cursor()

            print('-- Destroy {}--'.format(name))
            destroy = self.destroy(name)
            cursor.execute(destroy)

            print('-- Creating --')
            create = self.create(name, df)
            print(create)
            cursor.execute(create)
            print('-- Inserting --')
            insert = self.insert(name, df)
            print(insert)
            cursor.execute(insert)
            print('-- Done --\r\n\r\n')
            conn.close()
        except Exception as e:
            print(e)
            conn.close()


if __name__ == '__main__':
    files = []
    files.append('/home/kuzoncby/VirtualBoxVMs/Data/datasets/mtcars.csv')
    files.append('/home/kuzoncby/VirtualBoxVMs/Data/datasets/HairEyeColor.csv')
    files.append('/home/kuzoncby/VirtualBoxVMs/Data/datasets/EuStockMarkets.csv')

    hive = Hive()
    for file in files:
        hive.run(file)
