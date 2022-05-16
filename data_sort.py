import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# pd.set_option('display.max_rows', None) # 行
# pd.set_option('display.max_columns', None) # 列

now_date = datetime.now()
diff_date = now_date-relativedelta(days=30)
now_date = datetime.strftime(now_date, '%Y%m%d')
# diff_date = datetime.strftime(diff_date, '%Y%m%d')
print(diff_date)

# df = pd.read_csv(f'./data/recently_zeczec_{now_date}.csv')
df = pd.read_csv(f'./data/latest_all_zeczec_{now_date}.csv')

'''中文欄位'''
columns_name = [
    '集資項目',
    '提案人',
    '累積金額',
    '產品單價',
    '項目網址',
    '項目規格',
    '剩餘時間',
    '集資期間',
    '集資開始',
    '集資結束',
]

df.columns = columns_name
df['集資開始'] = df['集資開始'].astype('datetime64[ns]')
df['集資結束'] = df['集資結束'].astype('datetime64[ns]')
df = df[df['集資開始']>=diff_date]
limit_amount = 100000
df = df[df['累積金額']>=limit_amount]
# print(df.dtypes)
df = df.sort_values(by=['累積金額', '產品單價'], ascending=[False, False])
df.to_csv(f'./data/data_sort_zeczec_{now_date}.csv', mode='w', index=False)
print(df)


'''英文欄位'''
# columns_name = [
#     'projects',
#     'proposer',
#     'current_amount',
#     'current_price',
#     'projects_url',
#     'spec',
#     'remaining_time',
#     'group_period',
#     'group_period_start',
#     'group_period_end',
# ]

# df.columns = columns_name
# df['group_period_start'] = df['group_period_start'].astype('datetime64[ns]')
# df['group_period_end'] = df['group_period_start'].astype('datetime64[ns]')
# df = df[df['group_period_start']>=diff_date]
# # print(df.dtypes)
# df = df.sort_values(by=['current_amount', 'current_price'], ascending=[False, False])
# df.to_csv(f'./data/data_sort_zeczec_{now_date}.csv', mode='w', index=False)
# print(df)