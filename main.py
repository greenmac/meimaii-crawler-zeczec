from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv
import time
import os
import pandas as pd
import re
import requests

now_date = datetime.now()
now_date = datetime.strftime(now_date, '%Y%m%d')

domain_url = 'https://www.zeczec.com/'

def crawlerZeczecResults(time_sleep):
    soup = getSoup(domain_url)
    page = soup.select(
        '.container > .container > .text-center.mb5 > .button-group.mt3 > .button.button-s'
    )
    page = int(page[5].get_text())+1
        
    for i in range(1, page):
        categories_url = domain_url+f'?page={i}'
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        }
        getCrowdfundingInfo(categories_url, time_sleep)
    getRecentlyZeczecProjects()
    dataSort()
        
def getCrowdfundingInfo(categories_url, time_sleep):
    soup = getSoup(categories_url)
    soup_1 = soup.select(
        '.container > .container > .flex.gutter3-l > .w-full > .black.project > .block'
    )
    for i in soup_1:
        projects_href = i.get('href')
        projects_url = domain_url+projects_href
        getCategoriesInfo(projects_url, time_sleep)
             
def getCategoriesInfo(projects_url, time_sleep):
    projects_lists = []
    age_checked_for_lists = ['11454', '11518']
    for age_checked_for in age_checked_for_lists:
        check_exist_lists = checkExistLists()
        if projects_url in check_exist_lists:
            print('%'*20)
            print('Url exist:', projects_url)
        if projects_url not in check_exist_lists:
            print('%'*20)
            print('Url insert:', projects_url)
            cookies = {'age_checked_for' : age_checked_for}
            getProjectsInfo(projects_url, time_sleep, cookies)
            projects_lists.append(projects_url)
    len_projects_lits = len(projects_lists)
    print('$'*40)
    print('len_projects_lits:', len_projects_lits)
            
def getProjectsInfo(projects_url, time_sleep, cookies):
        soup = getSoup(projects_url, cookies=cookies)
        
        projects = soup.select('.f4.mt2.mb1')
        projects = projects[0].get_text() if projects else ''
        if projects:
            proposer = soup.select('.font-bold.f6')
            proposer = str(proposer[0].get_text()) if proposer else ''
            
            proposer_url = soup.select('.font-bold.f6')
            proposer_url = domain_url+proposer_url[0].get('href') if proposer else ''
                                    
            achievement_rate = soup.select('.stroke')
            achievement_rate = achievement_rate[0].get_text().replace('\n', '') if achievement_rate else ''
            
            current_amount = soup.select('.js-sum-raised')
            current_amount = current_amount[0].get_text() if current_amount else ''
            current_amount = int(''.join(re.findall('\d*', current_amount))) # 如果要轉換純數字
            
            target_amount = soup.select('.js-sum-raised') # 尋找 current_amount 的兄弟節點
            target_amount = target_amount[0].find_next_siblings()[0].get_text().replace('\n', '') if target_amount else '' # 尋找 current_amount 的兄弟節點
            target_amount = int(''.join(re.findall('\d*', target_amount))) if '目標' in target_amount else target_amount # 如果要轉換純數字

            current_price = soup.select('.black.font-bold.f4')
            current_price = current_price[0].get_text() if current_price else ''
            current_price = int(''.join(re.findall('\d*', current_price))) # 如果要轉換純數字

            spec = soup.select('.f6.gray.mv3')
            spec = spec[0].get_text()+',zeczec' if spec else ''
            
            number_of_sponsors = soup.select('.js-backers-count')
            number_of_sponsors = number_of_sponsors[0].get_text() if number_of_sponsors else ''
            
            remaining_time = soup.select('.js-time-left')
            remaining_time = remaining_time[0].get_text() if remaining_time else ''
            
            group_period = soup.select('.mb2.f7')
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
            print('='*20)
            print(projects_dict)
            filepath = f'./data/zeczec_projects_{now_date}.csv'
            df = pd.DataFrame(data=projects_dict, index=[0])
            df.to_csv(filepath, mode='a', header=False, index=False)
            time.sleep(time_sleep)
            return projects_dict

def checkExistLists():
    filepath = f'./data/zeczec_projects_{now_date}.csv'
    check_rows = []
    if os.path.isfile(filepath):
        with open(filepath, 'r', encoding="utf-8", newline='') as csvfile:
            rows = list(csv.reader(csvfile))
            for row in rows:
                check_rows.append(row[4])
    return check_rows

def getSoup(url, cookies=None):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    }
    resp = requests.session().get(url, headers=headers, cookies=cookies)
    resp_results = resp.text 
    soup = BeautifulSoup(resp_results, 'lxml')
    return soup

def getRecentlyZeczecProjects():
    with open(f'./data/zeczec_projects_{now_date}.csv', mode='r') as csv_file:
        data = list(csv.reader(csv_file))
        
    data_lists = []    
    for i in data:
        projects = i[0],
        proposer = i[1],
        current_amount = i[2],
        current_price = i[3],
        projects_url = i[4],
        spec = i[5],
        remaining_time = i[6],
        group_period = i[7]        
        group_period_lists = group_period.split(' – ')
        group_period_start = group_period_lists[0].replace('開始於', '')+':00'
        group_period_end = group_period_lists[1]+':00' if len(group_period_lists) == 2 else ''
        data_lists.append({
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
        
    data_lists_sort = sorted(data_lists, key=lambda k:k['group_period_start'], reverse=True)
    message = 'zeczec data does not conform !'
    for data in data_lists_sort:
        now_time = datetime.now()
        gap_time = now_time-relativedelta(days=7)
        # gap_time = datetime.strptime('2022/03/29 00:00:00', '%Y/%m/%d %H:%M:%S') # 如果要手動選擇某個日期開始
        
        group_period_start = data['group_period_start']
        group_period_start = datetime.strptime(group_period_start, '%Y/%m/%d %H:%M:%S')
        group_period_end = data['group_period_end']
        group_period_end = datetime.strptime(group_period_end, '%Y/%m/%d %H:%M:%S') if group_period_end else ''
        
        if group_period_end:
            filepath = f'./data/latest_all_zeczec_{now_date}.csv'
            df = pd.DataFrame(data=data, index=[0])
            df.to_csv(filepath, mode='a', header=False, index=False)
            print('@'*40)
            print(data)
            if gap_time<=group_period_start:
                filepath = f'./data/recently_zeczec_{now_date}.csv'
                df = pd.DataFrame(data=data, index=[0])
                df.to_csv(filepath, mode='a', header=False, index=False)
                print('@'*40)
                print(data)

        message = 'zeczec data is done !'
    print(message)

def dataSort():
    now_date = datetime.now()
    diff_date = now_date-relativedelta(days=30)
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
    df = df.sort_values(by=['累積金額', '產品單價'], ascending=[False, False])
    df.to_csv(f'./data/data_sort_zeczec_{now_date}.csv', mode='w', index=False)

if __name__ == '__main__':
    start_time = time.time()
    print('start_time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    crawlerZeczecResults(time_sleep=6)

    print('&'*80)
    end_time = time.time()
    print('end_time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    cost_time = end_time-start_time
    m, s = divmod(cost_time, 60)
    h, m = divmod(m, 60)
    print(f'cost_time: {int(h)}h:{int(m)}m:{round(s, 2)}s')
    