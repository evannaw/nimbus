import pandas as pd
import datetime as dt
import os
import numpy as np
import re
import openpyxl

pd.set_option('display.width', 1000, 'display.max_columns', 1000)

start_time = dt.datetime.now()
re_name = re.compile(r'xva\.(?P<env>[^\\.]+)\.(?P<role>[^\\.]+)\.(?P<index>[^\\.]+)\.(?P<disk>[^\\.]+)\.')
re_name2 = re.compile(r'arn\:aws\:(?P<region1>[^\\.]+)\:(?P<region>[^\\.]+)\:(?P<number>[^\\.]+)\:(?P<role>[^\\.]+)')


def process_csv(file):
    print(f'-   processing:   {file}  |  {dt.datetime.now() - start_time}')
    data = pd.read_csv(f'{csv_dir}/{file}', compression='zip')
    data.columns = [x.lower().replace(':', '_') for x in data.columns]
    data = data.loc[(data['user_mapid'] == 10147.0) | (data['user_project'].str.lower().str.contains('cva'))
                    | (data['resourceid'].str.lower().str.contains("cva"))
                    | (data['resourceid'].str.lower().str.contains("loadbalancer"))
                    | (data['user_name'].str.lower().str.contains('chef'))]
    data = data.loc[data['recordtype'] == 'LineItem']
    data = data[~(data['blendedcost'] == 0.0)]
    data = data['productname usageenddate user_environment blendedcost resourceid user_name usagetype'.split(' ')]

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

    map_product_name = {'Amazon Elastic Compute Cloud': 'VM',
                        'Amazon Relational Database Service': 'Database',
                        'Elastic Load Balancing': 'LoadBalancer',
                        'Amazon Simple Storage Service': 'Storage',
                        'AmazonCloudWatch': 'CloudWatch'}

    def role_col(x):
        env, role, index, disk = np.nan, np.nan, np.nan, np.nan

        match = re_name.search(x)

        if match:
            env = match.group('env')
            role = match.group('role')
            index = match.group('index')
            disk = match.group('disk')

        return [env, role, index, disk]

    def new_role(x):

        role, region, region1, number = np.nan, np.nan, np.nan, np.nan
        match2 = re_name2.match(x)
        if match2:
            role = match2.group('role')
            return role

    data['regex_match_new'] = data.resourceid.apply(lambda x: new_role(x))
    data['type'] = data.productname.apply(lambda x: map_product_name.get(x, 'Unknown'))

    data.usageenddate = pd.to_datetime(data.usageenddate, format='%Y-%m-%d %H:%M:%S', errors='coerce')

    data['week_number'] = data.usageenddate.apply(lambda x: x.weekofyear)
    data['week_day'] = data.usageenddate.apply(lambda x: x.dayofweek)
    data.set_index('usageenddate', inplace=True)

    data.user_name.fillna('', inplace=True)
    data['regex_match'] = data.user_name.apply(lambda x: role_col(x))
    data[['env_r', 'role_r', 'index', 'disk']] = pd.DataFrame(data.regex_match.tolist(), index=data.index)
    data['user_environment'] = np.where(data.env_r.isnull(), data.user_environment, data.env_r)
    data['role'] = np.where(data.role_r.isnull(), data.regex_match_new, data.role_r)
    data.type = np.where(data.role.str.startswith('loadbalancer'), 'Loadbalancer', data.type)
    # check = data['role'].str.contains('loadbalancer')
    # print(check)
    data.role = np.where(data.role.str.startswith('loadbalancer'), ' ', data.role)
    data.role = np.where(data.role.str.startswith('snapshot/snap-'), 'Snapshot', data.role)
    data.user_environment = data.user_environment.apply(lambda x: map_env.get(x, x))

    # Boundary cases
    data[['role', 'resourceid']] = data[['role', 'resourceid']].fillna('')
    data[['role', 'resourceid']] = data[['role', 'resourceid']].applymap(str.lower)

    data.type = np.where(data.resourceid.str.startswith('vol-'), 'Storage', data.type)
    data.type = np.where(data.role.str.startswith('snapshot'), 'AMI', data.type)
    data.role = np.where(data.resourceid.str.startswith('snap-'), 'Snapshot', data.role)
    data.role = np.where(data.user_name.str.lower().str.contains('chef'), 'chef-server', data.role)
    return data


csv_dir = '/Users/Evanna.W/Desktop/2020-09-25'
for _, _, files in os.walk(csv_dir):
    pass

files = [f for f in files if '.csv.zip' in f]

files = sorted(files)[-3:]
files = [process_csv(f) for f in files]

data = pd.concat(files, sort=False)
# data = data.groupby([pd.Grouper(freq='D'), 'user_environment']).agg({'blendedcost': 'sum'})
# data = data.unstack(1)
# data.columns = data.columns.droplevel()
# data['total'] = data.sum(axis=1)

# data.sort_index(ascending=False, inplace=True)
data = data.drop(['resourceid'], axis=1)
data = data.drop(['user_name'], axis=1)
data = data.drop(['productname'], axis=1)
data = data.drop(['regex_match'], axis=1)
data = data.drop(['regex_match_new'], axis=1)
data = data.drop(['env_r'], axis=1)
data = data.drop(['role_r'], axis=1)
data = data.drop(['disk'], axis=1)

data['role'].fillna(' ', inplace=True)
data['user_environment'].fillna(' ', inplace=True)
data['type'].fillna(' ', inplace=True)
data['index'].fillna(' ', inplace=True)
data['usagetype'].fillna(' ', inplace=True)

data = data.groupby(['role', 'type', 'user_environment', 'index', 'usagetype', 'week_number', 'week_day'])[
    "blendedcost"].sum()

from openpyxl import load_workbook

book = load_workbook('/Users/Evanna.W/Desktop/nimbus_report/_temp_pickles/dimo_version_newest.xlsx')
writer = pd.ExcelWriter('/Users/Evanna.W/Desktop/nimbus_report/_temp_pickles/dimo_version_newest.xlsx', engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

data.to_excel(writer, sheet_name='Env Detailed Daily Cost')

writer.save()
