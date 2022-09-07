from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import timer
import csv
import numpy as np
import os
import pandas as pd
import re
import requests
import time

now_date = datetime.now()
now_date = datetime.strftime(now_date, '%Y%m%d')

domain_url = 'https://www.zeczec.com'

@timer
def crawler_zeczec_results(time_sleep):
    soup = get_soup(domain_url)
    page = soup.select(
        '.container > .container > .text-center.mb-16 > .button-group.mt-4 > .button.button-s'
    )
    page = int(page[5].get_text())+1        
    for i in range(1, page):
        categories_url = domain_url+'/'+f'?page={i}'
        get_crowdfunding_info(categories_url, time_sleep)

    get_df_add_header_to_csv()
    get_recently_zeczec_projects()
    data_sort()
        
def get_crowdfunding_info(categories_url, time_sleep):
    soup = get_soup(categories_url)
    projects_url_list = soup.select(
        '.container > .container > .flex.gutter3-l > .w-full > .text-black > .block'
    )
    for i in projects_url_list:        
        projects_href = i.get('href')
        projects_url = domain_url+projects_href
        get_check_projects_url(projects_url, time_sleep)

def get_check_projects_url(projects_url, time_sleep):
    check_exist_list = get_check_exist_list()
    if projects_url in check_exist_list:
        print('%'*20)
        print('Url exist:', projects_url)
    if projects_url not in check_exist_list:
        print('%'*20)
        print('Url insert:', projects_url)
        get_projects_info(projects_url, time_sleep)
            
def get_projects_info(projects_url, time_sleep):
    cookies = get_cookies(projects_url)
    
    soup = get_soup(projects_url, cookies=cookies)
    
    projects = soup.select('.mt-2.mb-1')
    projects = projects[0].get_text() if projects else ''

    proposer = soup.select('.font-bold.text-sm')
    proposer = str(proposer[0].get_text()) if proposer else ''
    
    proposer_url = soup.select('.font-bold.text-sm')
    proposer_url = domain_url+proposer_url[0].get('href') if proposer else ''
                            
    achievement_rate = soup.select('.stroke')
    achievement_rate = achievement_rate[0].get_text().replace('\n', '') if achievement_rate else ''
    
    current_amount = soup.select('.js-sum-raised')
    current_amount = current_amount[0].get_text() if current_amount else ''
    current_amount = int(''.join(re.findall('\d*', current_amount))) if current_amount else '' # 如果要轉換純數字
    
    target_amount = soup.select('.js-sum-raised') # 尋找 current_amount 的兄弟節點
    target_amount = target_amount[0].find_next_siblings()[0].get_text().replace('\n', '') if target_amount else '' # 尋找 current_amount 的兄弟節點
    target_amount = int(''.join(re.findall('\d*', target_amount))) if '目標' in target_amount else target_amount # 如果要轉換純數字

    current_price = soup.select('.text-black.font-bold.text-xl')
    current_price = current_price[0].get_text() if current_price else ''
    current_price = int(''.join(re.findall('\d*', current_price))) if current_price else ''# 如果要轉換純數字

    spec = soup.select('.text-sm.text-neutral-600.my-4.leading-relaxed')
    spec = spec[0].get_text()+',zeczec' if spec else ''
    
    number_of_sponsors = soup.select('.js-backers-count')
    number_of_sponsors = number_of_sponsors[0].get_text() if number_of_sponsors else ''
    
    remaining_time = soup.select('.js-time-left')
    remaining_time = remaining_time[0].get_text() if remaining_time else ''
    
    group_period = soup.select('.mb-2.text-xs.leading-relaxed')
    group_period = group_period[0].get_text().replace('\n', '').replace('時程', '') if group_period else ''

    projects_dict = {
        'projects': projects,
        'proposer': proposer,
        'current_amount': current_amount,
        'current_price': current_price,
        'projects_url': projects_url,
        'spec': spec,
        'remaining_time': remaining_time,
        'group_period': group_period,  
        # 'proposer_url': proposer_url,
        # 'achievement_rate': achievement_rate,
        # 'target_amount': target_amount,
        # 'number_of_sponsors': number_of_sponsors,
    }
    print('-'*10)
    print(projects_dict)
    filepath = f'./data/zeczec_projects_{now_date}.csv'
    df = pd.DataFrame(data=projects_dict, index=[0])
    df.to_csv(filepath, mode='a', header=False, index=False)
    time.sleep(time_sleep)
    return projects_dict

def get_check_exist_list():
    filepath = f'./data/zeczec_projects_{now_date}.csv'
    check_rows = []
    if os.path.isfile(filepath):
        with open(filepath, 'r', encoding="utf-8", newline='') as csvfile:
            rows = list(csv.reader(csvfile))
            check_rows = [row[4] for row in rows]
    return check_rows

def get_soup(url, cookies=None):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    }
    resp = requests.session().get(url, headers=headers, cookies=cookies)
    resp_results = resp.text 
    soup = BeautifulSoup(resp_results, 'lxml')
    return soup

def get_cookies(url):
    soup = get_soup(url)
    projects = soup.select('.mt-2.mb-1')
    projects = projects[0].get_text() if projects else ''
    age_checked_for = '' if projects else '12925'
    return {'age_checked_for' : age_checked_for}

def get_recently_zeczec_projects():
    file_path = f'./data/zeczec_projects_{now_date}.csv'
    df = pd.read_csv(file_path).replace({np.nan:None})
    df_dict_list = df.to_dict('records')    

    data_list = []
    for i in df_dict_list:
        projects = i['projects']
        proposer = i['proposer']
        current_amount = i['current_amount']
        current_price = i['current_price']
        projects_url = i['projects_url']
        spec = i['spec']
        remaining_time = i['remaining_time']
        group_period = i['group_period']
        group_period_lists = group_period.split(' – ') if group_period else ''
        group_period_start = group_period_lists[0].replace('開始於', '')+':00' if group_period_lists else ''
        group_period_end = group_period_lists[1]+':00' if len(group_period_lists) == 2 else ''
        data_list.append({
            'projects': projects,
            'proposer': proposer,
            'current_amount': current_amount,
            'current_price': current_price,
            'projects_url': projects_url,
            'spec': spec,
            'remaining_time': remaining_time,
            'group_period': group_period,
            'group_period_start': group_period_start,
            'group_period_end': group_period_end,
        })
        
    data_list_sort = sorted(data_list, key=lambda k:k['group_period_start'], reverse=True)
    message = 'The zeczec data does not conform !'
    latest_all_zeczec_data = []
    recently_zeczec_data = []
    for data in data_list_sort:
        now_time = datetime.now()
        gap_time = now_time-relativedelta(days=7)
        # gap_time = datetime.strptime('2022/03/29 00:00:00', '%Y/%m/%d %H:%M:%S') # 如果要手動選擇某個日期開始
        
        group_period_start = data['group_period_start']
        group_period_start = datetime.strptime(group_period_start, '%Y/%m/%d %H:%M:%S') if group_period_start else ''
        group_period_end = data['group_period_end']
        group_period_end = datetime.strptime(group_period_end, '%Y/%m/%d %H:%M:%S') if group_period_end else ''
        
        latest_all_zeczec_data.append(data)
        if group_period_end and gap_time<=group_period_start:
            recently_zeczec_data.append(data)

        message = 'The zeczec data is done !'
        
    filepath_latest = f'./data/latest_all_zeczec_{now_date}.csv'
    df_latest = pd.DataFrame(data=latest_all_zeczec_data)
    df_latest.to_csv(filepath_latest, mode='w', index=False)

    filepath_recently = f'./data/recently_zeczec_{now_date}.csv'
    df_recently = pd.DataFrame(data=recently_zeczec_data)
    df_recently.to_csv(filepath_recently, mode='w', index=False)
    
    print('@'*30)
    print(f'latest_all_zeczec_data:{latest_all_zeczec_data}')
    print('@'*30)    
    print(message)

def get_df_add_header_to_csv():
    columns_name = [
        'projects',
        'proposer',
        'current_amount',
        'current_price',
        'projects_url',
        'spec',
        'remaining_time',
        'group_period',  
    ]
    file_path = f'./data/zeczec_projects_{now_date}.csv'
    df = pd.read_csv(file_path, header=None)
    df.columns = columns_name
    df.to_csv(file_path, index=False)

def data_sort():
    now_date = datetime.now()
    diff_date = now_date-relativedelta(days=30) # 取30天內開團的商品
    now_date = datetime.strftime(now_date, '%Y%m%d')

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
        '集資期間(30天內開團的商品)',
        '集資開始',
        '集資結束',
    ]

    df.columns = columns_name
    df['集資開始'] = df['集資開始'].astype('datetime64[ns]')
    df['集資結束'] = df['集資結束'].astype('datetime64[ns]')
    df = df[df['集資開始']>=diff_date] # 取30天內開團的商品
    limit_amount = 500000 # 限制多少金額才列出
    df = df[df['累積金額']>=limit_amount]
    df = df.sort_values(by=['累積金額'], ascending=[False])
    df.to_csv(f'./data/data_sort_zeczec_{now_date}.csv', mode='w', index=False)

if __name__ == '__main__':    
    crawler_zeczec_results(time_sleep=6)
