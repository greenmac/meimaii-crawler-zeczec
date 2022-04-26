import pandas as pd
from datetime import datetime

# pd.set_option('display.max_rows', None) # 行
# pd.set_option('display.max_columns', None) # 列

now_date = datetime.now()
now_date = datetime.strftime(now_date, '%Y%m%d')

df = pd.read_csv(f'./data/recently_zeczec_{now_date}.csv')
columns_name = [
    'projects',
    'proposer',
    'current_amount',
    'current_price',
    'projects_url',
    'spec',
    'remaining_time',
    'group_period',
    'group_period_start',
    'group_period_end',
]
df.columns = columns_name
df = df.sort_values(by=['current_amount', 'current_price'], ascending=[False, False])
df.to_csv(f'./data/data_sort_zeczec_{now_date}.csv', mode='w', index=False)
print(df)