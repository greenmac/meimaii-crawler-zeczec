from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import timer
import csv
import logging
import numpy as np
import os
import pandas as pd
import re
import requests
import time

now_date = datetime.strftime(datetime.now(), '%Y%m%d')

web_name = 'zeczec'
file_path = f'./data/recently_{web_name}_{now_date}.csv'

domain_url = 'https://www.zeczec.com'

@timer
def crawler_zeczec_results(time_sleep):
    soup = get_soup(domain_url)

    # 群眾集資
    page = soup.select('.container > .container > .text-center.mb-16 > .button-group.mt-4 > .button.button-s')
    page = int(page[5].get_text())+1
    for i in range(1, page):
        crowdfunding_url = domain_url+'/'+f'?page={i}'
        get_crowdfunding_info(crowdfunding_url, time_sleep)

    # 預購式專案
    p_page = soup.select('.border-t > .container > .text-center.mb-16 > .button-group.mt-4')[0]
    p_page = p_page.select('.button.button-s')
    p_page = int(p_page[5].get_text())+1
    for i in range(1, p_page):
        pre_order_url = domain_url+'/'+f'?p_page={i}'
        get_pre_order_info(pre_order_url, time_sleep)

    get_df_add_header_to_csv()
    data_sort()
    amount_limit()
        
def get_crowdfunding_info(crowdfunding_url, time_sleep):
    soup = get_soup(crowdfunding_url)
    projects_url_list = soup.select('.container > .container > .flex.gutter3-l > .w-full > .text-black > .block')
    for i in projects_url_list:        
        projects_href = i.get('href')
        projects_url = domain_url+projects_href
        get_check_projects_url(projects_url, time_sleep)

def get_pre_order_info(pre_order_url, time_sleep):
    soup = get_soup(pre_order_url)
    projects_url_list = soup.select('.border-t > .container > .flex.gutter3-l')[0]
    projects_url_list = projects_url_list.select('.w-full > .text-black > .block')
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
    soup = get_cookies_soup(projects_url)
    
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

    current_price = soup.select('div.text-black.font-bold.text-xl')
    current_price = current_price[0].get_text().strip().split('\n')[0] if current_price else ''
    current_price = int(''.join(re.findall('\d*', current_price))) if current_price else ''# 如果要轉換純數字

    spec = soup.select('.text-sm.text-neutral-600.my-4.leading-relaxed')
    spec = spec[0].get_text()+',zeczec' if spec else ''
    
    number_of_sponsors = soup.select('.js-backers-count')
    number_of_sponsors = number_of_sponsors[0].get_text() if number_of_sponsors else ''
    
    remaining_time = soup.select('.js-time-left')
    remaining_time = remaining_time[0].get_text() if remaining_time else ''
    
    group_period = soup.select('.mb-2.text-xs.leading-relaxed')
    group_period = group_period[0].get_text().replace('\n', '').replace('時程', '') if group_period else ''
            
    group_period_lists = group_period.split(' – ') if group_period else ''

    group_period_start = group_period_lists[0]+':00' if group_period_lists else ''
    
    group_period_end = group_period_lists[1]+':00' if len(group_period_lists) == 2 else ''
    
    last_week_date = datetime.date(datetime.now())-relativedelta(days=7)
    last_week_date = datetime.strftime(last_week_date, '%Y%m%d')
    df_last_week = pd.read_csv(f'./data/recently_{web_name}_{last_week_date}.csv')
    df_last_week_url_list = df_last_week.values.tolist()
    df_last_week_url_list = [i[4] for i in df_last_week_url_list]
    new_product = '✅' if projects_url not in df_last_week_url_list else ''
    
    projects_dict = {
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
        'new_product': new_product,
        # 'proposer_url': proposer_url, # 暫時不顯示
        # 'achievement_rate': achievement_rate, # 暫時不顯示
        # 'target_amount': target_amount, # 暫時不顯示
        # 'number_of_sponsors': number_of_sponsors, # 暫時不顯示
    }
    print('-'*10)
    print(projects_dict)
    df = pd.DataFrame(data=projects_dict, index=[0])
    df.to_csv(file_path, mode='a', header=False, index=False)
    time.sleep(time_sleep)
    return projects_dict

def get_projects_info_logging(projects_url, time_sleep): # 如果需要用log取代錯誤訊息，要把 get_projects_info(projects_url, time_sleep) 取代掉
    try:
        soup = get_cookies_soup(projects_url)
        
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

        current_price = soup.select('div.text-black.font-bold.text-xl')
        current_price = current_price[0].get_text().strip().split('\n')[0] if current_price else ''
        current_price = int(''.join(re.findall('\d*', current_price))) if current_price else ''# 如果要轉換純數字

        spec = soup.select('.text-sm.text-neutral-600.my-4.leading-relaxed')
        spec = spec[0].get_text()+',zeczec' if spec else ''
        
        number_of_sponsors = soup.select('.js-backers-count')
        number_of_sponsors = number_of_sponsors[0].get_text() if number_of_sponsors else ''
        
        remaining_time = soup.select('.js-time-left')
        remaining_time = remaining_time[0].get_text() if remaining_time else ''
        
        group_period = soup.select('.mb-2.text-xs.leading-relaxed')
        group_period = group_period[0].get_text().replace('\n', '').replace('時程', '') if group_period else ''
                
        group_period_lists = group_period.split(' – ') if group_period else ''

        group_period_start = group_period_lists[0]+':00' if group_period_lists else ''
        
        group_period_end = group_period_lists[1]+':00' if len(group_period_lists) == 2 else ''
        
        last_week_date = datetime.date(datetime.now())-relativedelta(days=7)
        last_week_date = datetime.strftime(last_week_date, '%Y%m%d')
        df_last_week = pd.read_csv(f'./data/recently_{web_name}_{last_week_date}.csv')
        df_last_week_url_list = df_last_week.values.tolist()
        df_last_week_url_list = [i[4] for i in df_last_week_url_list]
        new_product = '✅' if projects_url not in df_last_week_url_list else ''
        
        projects_dict = {
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
            'new_product': new_product,
            # 'proposer_url': proposer_url, # 暫時不顯示
            # 'achievement_rate': achievement_rate, # 暫時不顯示
            # 'target_amount': target_amount, # 暫時不顯示
            # 'number_of_sponsors': number_of_sponsors, # 暫時不顯示
        }
        print('-'*10)
        print(projects_dict)
        df = pd.DataFrame(data=projects_dict, index=[0])
        df.to_csv(file_path, mode='a', header=False, index=False)
        time.sleep(time_sleep)
        return projects_dict
    except Exception as e:
        logging.basicConfig(
            level=logging.INFO, 
            filename=f'./log/log_{web_name}.txt', filemode='w',
            format='[%(asctime)s] %(message)s', 
            datefmt='%Y%m%d %H:%M:%S',
        )
        logging_message = f'projects_url:{projects_url}, {e}'
        logging.error(msg=logging_message)

def get_check_exist_list():
    check_rows = []
    if os.path.isfile(file_path):
        with open(file_path, 'r', encoding="utf-8", newline='') as csvfile:
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
    if cookies:
        time.sleep(3) # 如果cookies 有傳值進來的話，停頓秒數設置
    return soup

def get_cookies_soup(url):
    age_checked_for_list = ['', '14118', '12925', '10649', '13819', '14047']
    for age_checked_for in age_checked_for_list:
        soup = get_soup(url, cookies={'age_checked_for': age_checked_for})
        projects = soup.select('.mt-2.mb-1') if soup else ''
        if projects:
            return soup
        else:
            time.sleep(9)
    return get_soup(url)

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
        'group_period_start',
        'group_period_end',
        'new_product',
    ]
    df = pd.read_csv(file_path, header=None)
    df_temp = df[0:1][0].values
    if 'projects' in df_temp:
        df = pd.read_csv(file_path)
    if 'projects' not in df_temp:
        df.columns = columns_name
    df.to_csv(file_path, index=False)

def data_sort():
    now_date = datetime.now()
    diff_date = datetime.strftime(now_date-relativedelta(days=30), '%Y-%m-%d')+' 23:59:59' # 取30天內開團的商品, 設定到 YYYY-mm-dd 23:59:59
    diff_date = datetime.strptime(diff_date, '%Y-%m-%d %H:%M:%S')

    now_date = datetime.strftime(now_date, '%Y%m%d')

    df = pd.read_csv(file_path)

    '''中文欄位'''
    columns_name = [
        '商品名稱',
        '品牌名稱',
        '累積金額',
        '商品單價',
        '商品網址',
        '商品規格',
        '剩餘時間',
        '集資期間(30天內開團的商品)',
        '集資開始',
        '集資結束',
        '新品入榜',
    ]

    df.columns = columns_name
    df['集資開始'] = df['集資開始'].astype('datetime64[ns]')
    df['集資結束'] = df['集資結束'].astype('datetime64[ns]')
    df = df[df['集資開始']>=diff_date] # 取30天內開團的商品
    limit_amount = 500000 # 限制多少金額才列出
    df = df[df['累積金額']>=limit_amount]
    df = df.sort_values(by=['新品入榜', '累積金額'], ascending=[False, False])
    df.to_csv(f'./data/data_sort_{web_name}_{now_date}.csv', mode='w', index=False)

def amount_limit():
    now_date = datetime.now()
    diff_date = datetime.strftime(now_date, '%Y-%m-%d')+' 23:59:59' # 取爬蟲當天內還開團的商品, 設定到 YYYY-mm-dd 23:59:59
    diff_date = datetime.strptime(diff_date, '%Y-%m-%d %H:%M:%S')
    now_date = datetime.strftime(now_date, '%Y%m%d')

    df = pd.read_csv(file_path)

    '''中文欄位'''
    columns_name = [
        '商品名稱',
        '品牌名稱',
        '累積金額',
        '商品單價',
        '商品網址',
        '商品規格',
        '剩餘時間',
        f'集資期間({now_date} 截止)',
        '集資開始',
        '集資結束',
        '新品入榜',
    ]

    df.columns = columns_name
    df['集資結束'] = df['集資結束'].astype('datetime64[ns]')
    df = df[df['集資結束']>=diff_date] # 取爬蟲當天內還開團的商品
    limit_amount = 5000000 # 限制多少金額才列出
    df = df[df['累積金額']>=limit_amount]
    df = df.sort_values(by=['新品入榜', '累積金額'], ascending=[False, False])
    df.to_csv(f'./data/amount_limit_{web_name}_{now_date}.csv', mode='w', index=False)

if __name__ == '__main__':    
    crawler_zeczec_results(time_sleep=9)
    # get_projects_info('https://www.zeczec.com/projects/esensepro', 0)
    