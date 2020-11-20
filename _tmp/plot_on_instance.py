import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import datetime as dt
import os
import sys
import pickle

pd.set_option('display.width', 1000, 'display.max_column', 1000, 'display.max_rows', 2000)
#change the default number of rows to be displayed
import warnings
# used to show warning
warnings.filterwarnings('ignore')

df = pd.read_pickle('final_df.p')
df = df.reset_index()
df = df[df['Normalized'] == 'Virtual Machine']
print(df.drop_duplicates(['UsageType', 'user:environment']
                         , keep='first'))


def class_of_server(x):
    x = x.lower()

    if 'computenode' in x:
        return "Compute Node"
    elif 'headnode' in x:
        return "Head Node"
    elif 'cvaserver' in x:
        return "CVA Server"
    elif 'quote' in x:
        return "Quote Node"
    elif 'data' in x:
        return "Data Node"
    elif 'packer' in x:
        return "Deployment CI"
    elif 'chef' in x:
        return "Deployment Chef"
    else:
        return "Unmapped"


df['UsageType'] = "(" + df['user:environment'] + ") " + df['UsageType']
df['UsageType'] = df['UsageType'].apply(lambda x: x.replace('BoxUsage:', '\n'))
df['UsageType'] = df['UsageType'].apply(lambda x: x.replace('EBS:VolumeUsage:', '\n'))
df['UsageType'] = df['UsageType'].apply(
    lambda x: x.replace('DedicatedUsage:', '\nDedicated ').replace('DedicatedUsage', '\nDedicated'))

df['server_class'] = df['user:Name'].apply(lambda x: class_of_server(x))
df['server_class'] = np.where(df.UsageType.str.contains('EBS:VolumeUsage'), 'EBS Volume', df.server_class)
df['server_class'] = np.where(df.UsageType.str.contains('Out-Bytes'), 'Unmapped', df.server_class)
print(df[df['server_class'] == 'Unmapped'])
df = df[~(df['server_class'] == 'Unmapped')]
df = df[~(df['UsageType'].str.contains('DataTransfer'))]

print(df)

#

df = df['UsageStartDate UsageType BlendedCost server_class'.split(' ')]
df.set_index('UsageStartDate', inplace=True)
df = df.groupby([pd.Grouper(freq='M'), 'server_class', 'UsageType']).sum()
print(df)
# sys.exit()

# df['user:environment'] = df['user:environment'].apply(lambda x: x.strip().replace('_', '-'))
df = df.unstack(0).T

with pd.ExcelWriter('server.xlsx') as f:
    df.to_excel(f, 'data')

print(df)
# sys.exit()
#
df = df.set_index('UsageStartDate',).drop(['UsageType', 'user:Name', 'Normalized'], axis=1).groupby(
    [pd.Grouper(freq='M'), 'user:environment']).sum()
df = df.unstack(1)
df.columns = df.columns.get_level_values(1)

