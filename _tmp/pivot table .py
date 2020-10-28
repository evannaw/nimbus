



import pandas as pd
import datetime as dt
import os
import xlsxwriter
import re

pd.set_option('display.width', 1000, 'display.max_columns', 1000)

data = pd.read_csv(
    '/Users/Evanna.W/Desktop/2020-09-25/130019805858-aws-billing-detailed-line-items-with-resources-and-tags-2020-09.csv.zip',
    compression='zip')
# data = pd.read_csv('/Users/Evanna.W/Desktop/test.csv')

data.columns = [x.lower().replace(':', '_') for x in data.columns]

data = data.loc[(data['user_mapid'] == 10147.0) | (data['user_project'].str.lower() == 'cva')]
data = data[data['recordtype'] == 'LineItem']
data = data[~(data['blendedcost'] == 0.0)]
# data.to_pickle('../routines/raw_2020_09.p')

# data = pd.read_pickle('/Users/Evanna.W/Desktop/2020-09-25/routines/raw_2020_09.p')
start_time = dt.datetime.now()


def process_csv(file):
    print(f'-   processing:   {file}  |  {dt.datetime.now() - start_time}')
    data = pd.read_csv(f'{csv_dir}/{file}', compression='zip')
    data.columns = [x.lower().replace(':', '_') for x in data.columns]
    data = data.loc[(data['user_mapid'] == 10147.0) | (data['user_project'].str.lower() == 'cva')]
    data = data[data['recordtype'] == 'LineItem']
    data = data[~(data['blendedcost'] == 0.0)]
    data = data['productname usageenddate user_environment blendedcost resourceid user_name'.split(' ')]
    data = data.rename(
        columns={'productname': 'product_name', 'usageenddate': 'end_date', 'resourceid': 'resource',
                 'user_environment': 'env',
                 'blendedcost': 'cost'})

    map_env = {'dev_test_5': 'dev-5',
               'dev_test_1': 'dev-1',
               'dev_test_2': 'dev-2',
               'dev_test_3': 'dev-3',
               'dev_test_4': 'dev-4',
               'dev_test_6': 'dev-6',
               'dev_test_7': 'dev-7',
               'dev_ci': 'dev-8',
               'uat_b': 'uat-b',
               'prod_b': 'prod-b',
               }

    map_product = {'Amazon Elastic Compute Cloud': 'EC2',
                   'Amazon Simple Storage Service': 'S3',
                   'Elastic Load Balancing': 'LoadBalance'
                   }

    data.env = data.env.apply(lambda x: map_env.get(x, x))
    data.product_name = data.product_name.apply(lambda x: map_product.get(x, x))
    data.end_date = pd.to_datetime(data.end_date, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    data['week_number'] = data.end_date.apply(lambda x: x.weekofyear)
    data['week_day'] = data.end_date.apply(lambda x: x.dayofweek)
    data.set_index('end_date', inplace=True)

    return data


csv_dir = '/Users/Evanna.W/Desktop/2020-09-25'
for _, _, files in os.walk(csv_dir):
    pass

files = [f for f in files if '.csv.zip' in f]
files = sorted(files)[-3:]
files = [process_csv(f) for f in files]
# files = [process_csv('test.csv.zip')]

data = pd.concat(files, sort=False)
# data = data.groupby([pd.Grouper(freq='D'), 'env', 'product_name'])
# data = data.unstack(1)
# data = data.xs('EC2', level='product_name')
# data.columns = data.columns.droplevel()
user = data.loc[:, ['user_name']]
roles = []
indices = []
# x = re.search("xva\.(?<env>[^\.]+)\.(?<role>[^\.]+)\.(?<index>[^\.]+)\.(?<disk>[^\.]+)\.", RegexOptions.Compiled)
for index, row in user.iterrows():
    if type(row['user_name']) is str: #we are lossing data here
        current = row['user_name'].split('.')
        if len(current) >= 4:
            roles.append(current[2])
            indices.append(current[3])
        else:
            roles.append('')
            indices.append('')
    else:
        roles.append('')
        indices.append('')
# print(roles)
data['roles'] = roles
data['indices'] = indices

data = data.drop(['user_name'], axis=1)
data = data.drop(['product_name'], axis=1)
data = data.drop(['resource'], axis=1)
data = data.pivot_table(index=['roles', 'indices', 'env'], columns=['week_number', 'week_day'], values='cost')
print(data)

writer = pd.ExcelWriter('daily_cva.xlsx')
data.to_excel(writer, sheet_name='Env Daily Usage Time Report')
workbook = writer.book
worksheet = writer.sheets['Env Daily Usage Time Report']

# worksheet.autofilter(0, 0, 0, 3)
# worksheet.filter_column_list('week_number', [38, 39])
# row = 1
# for row_data in data:
#     worksheet.write_row(row, 0, row_data)
#     row += 1

workbook.close()