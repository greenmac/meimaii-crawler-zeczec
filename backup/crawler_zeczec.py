from bs4 import BeautifulSoup
from datetime import datetime
import csv
import os
import pandas as pd
import requests
import time

now_date = datetime.now()
now_date = datetime.strftime(now_date, '%Y%m%d')

def crawlerZeczecResults(time_sleep):
    url = f'https://www.zeczec.com/'
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    }
    resp = requests.session().get(url, headers=headers)
    resp_result = resp.text
    soup = BeautifulSoup(resp_result, 'lxml')
    p_page = int(soup.select('.dib-ns')[5].get_text())+1
    
    projects_lists = []
    for i in range(1, p_page):
        url = f'https://www.zeczec.com/?p_page={i}'
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        }
        resp = requests.session().get(url, headers=headers)
        resp_result = resp.text
        soup = BeautifulSoup(resp_result, 'lxml')

        soup_1 = soup.select('.project')
        for i in soup_1:
            soup_2 = i.select('.db')[0]
            projects_url = 'https://www.zeczec.com'+soup_2.get('href')
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
                    resp_sub = requests.get(projects_url, headers=headers, cookies=cookies)
                    resp_result_sub = resp_sub.text
                    soup_sub = BeautifulSoup(resp_result_sub, 'lxml')
                    
                    projects = soup_sub.select('.f4.mt2.mb1')
                    projects = projects[0].get_text() if projects else ''
                    if projects:
                        proposer = soup_sub.select('.b.f6')
                        proposer = str(proposer[0].get_text()) if proposer else ''
                        
                        proposer_url = soup_sub.select('.b.f6')
                        proposer_url = 'https://www.zeczec.com'+proposer_url[0].get('href') if proposer else ''
                                                
                        achievement_rate = soup_sub.select('.stroke')
                        achievement_rate = achievement_rate[0].get_text().replace('\n', '') if achievement_rate else ''
                        
                        current_amount = soup_sub.select('.js-sum-raised')
                        current_amount = current_amount[0].get_text() if current_amount else ''
                        # current_amount = int(''.join(re.findall('\d+', current_amount))) # 如果要轉換純數字
                        
                        target_amount = soup_sub.select('.js-sum-raised') # 尋找 current_amount 的兄弟節點
                        target_amount = target_amount[0].find_next_siblings()[0].get_text().replace('\n', '') if target_amount else '' # 尋找 current_amount 的兄弟節點
                        # # target_amount = int(''.join(re.findall('\d+', target_amount))) # 如果要轉換純數字
                        
                        number_of_sponsors = soup_sub.select('.js-backers-count')
                        number_of_sponsors = number_of_sponsors[0].get_text() if number_of_sponsors else ''
                        
                        remaining_time = soup_sub.select('.js-time-left')
                        remaining_time = remaining_time[0].get_text() if remaining_time else ''
                        
                        group_period = soup_sub.select('.mb2')
                        group_period = group_period[0].get_text().replace('\n', '').replace('時程', '') if group_period else ''

                        projects_dict = {
                            'projects': projects,
                            'projects_url': projects_url,
                            'proposer': proposer,
                            'proposer_url': proposer_url,
                            'achievement_rate': achievement_rate,
                            'current_amount': current_amount,
                            'target_amount': target_amount,
                            'number_of_sponsors': number_of_sponsors,
                            'remaining_time': remaining_time,
                            'group_period': group_period,
                        }
                        projects_lists.append(projects_dict)
                        print('='*20)
                        print(projects_dict)
                        filepath = f'./data/zeczec_projects_{now_date}.csv'
                        df = pd.DataFrame(data=projects_dict, index=[0])
                        df.to_csv(filepath, mode='a', header=False, index=False)
                        
                        time.sleep(time_sleep)

    len_projects_lits = len(projects_lists)
    print('$'*40)
    print('len_projects_lits:', len_projects_lits)
    return projects_lists

def checkExistLists():
    filepath = f'./data/zeczec_projects_{now_date}.csv'
    check_rows = []
    if os.path.isfile(filepath):
        with open(filepath, 'r', encoding="utf-8", newline='') as csvfile:
            rows = list(csv.reader(csvfile))
            for row in rows:
                check_rows.append(row[1])
    return check_rows

if __name__ == '__main__':
    start_time = time.time()
    print('start_time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    crawlerZeczecResults(time_sleep=6)

    print('&'*80)
    end_time = time.time()
    print('end_time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('cost_time:', end_time-start_time)