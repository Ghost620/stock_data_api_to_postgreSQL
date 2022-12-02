import requests
import json
import re
import pandas as pd
import time
import psycopg2
import hashlib
from dotenv import load_dotenv
from functools import wraps
import os
import smtplib
from sshtunnel import SSHTunnelForwarder
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def send_mail(send_from: str,
              subject: str,
              text: str,
              send_to: list,
              files=None):
    send_to = default_address if not send_to else send_to
    msg = MIMEMultipart()
    username = 'faghost6201@gmail.com'
    password = 'mochdbbyiiwmxzzg'
    msg['From'] = send_from
    msg['To'] = ', '.join(send_to)
    msg['Subject'] = subject
    part1 = MIMEText(text, 'plain')
    msg.attach(part1)
    for f in files or []:
        with open(f, "rb") as fil:
            ext = f.split('.')[-1:]
            attachedfile = MIMEApplication(fil.read(), _subtype=ext)
            attachedfile.add_header('content-disposition',
                                    'attachment',
                                    filename=basename(f))
        msg.attach(attachedfile)

    if '@gmail' in send_from:
        smtp = smtplib.SMTP(host="smtp.gmail.com", port=587)
    elif '@yahoo' in send_from:
        smtp = smtplib.SMTP(host="smtp.mail.yahoo.com", port=587)
    elif '@outlook' in send_from or '@hotmail' in send_from:
        smtp = smtplib.SMTP(host="smtp-mail.outlook.com", port=587)
    elif '@verizonwireless' in send_from:
        smtp = smtplib.SMTP(host="smtp.verizon.net", port=465)

    smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()
def replace_values(list_to_replace, item_to_replace, item_to_replace_with):
    return [item_to_replace_with if item == item_to_replace else item for item in list_to_replace]



DB_ENV_PROD=1

load_dotenv()
api_key=os.getenv('API_KEY')

if DB_ENV_PROD==0:
    
    database=os.getenv('LOCAL_DB_NAME')
    user=os.getenv('LOCAL_DB_USER')
    password=os.getenv('LOCAL_DB_PASSWORD')
    port=os.getenv('DB_PORT')
    host=os.getenv('DB_HOST')
    


    def database_connection(host=host,database=database,user=user,password=password,port=port):
        conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port)

        return conn

    make_table_query='''
    CREATE TABLE IF NOT EXISTS share_float (
                hashed_key text NOT NULL PRIMARY KEY,
                symbol VARCHAR(255),
                date TIMESTAMP,
                free_float float(24),
                float_shares float(24),
                outstanding_shares float(24),
                exchange VARCHAR(255),
                stock_exchange VARCHAR(255),
                country VARCHAR(255)
            );
    '''


    conn=database_connection()
    cur = conn.cursor()
    cur.execute(make_table_query)
    conn.commit()
    cur.close()

    response_stock_list=requests.get(f'https://fmpcloud.io/api/v3/stock/list?apikey={api_key}')
    data_stock_list=response_stock_list.json()
    required_companies=[]
    required_companies_symbol=[]

    options = uc.ChromeOptions()
    options.headless=True
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')


    driver = uc.Chrome(service=Service(ChromeDriverManager().install()),use_subprocess=True,options=options)
    driver.get('https://fmpcloud.io/api/v3')
    driver.implicitly_wait(30)


    for company in data_stock_list:
        if (company['exchangeShortName']=='NASDAQ') or (company['exchangeShortName']=='NYSE'):  
            specific_stock=driver.execute_script('''
                var datas
                await fetch("https://fmpcloud.io/api/v3/profile/'''+company["symbol"]+'''?apikey='''+api_key+'''", {
                "headers": {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1"
              },
              "referrerPolicy": "strict-origin-when-cross-origin",
              "body": null,
              "method": "GET",
              "mode": "cors",
              "credentials": "include"
            }).then((response) => response.json()).then((data)=>datas=data)
            return datas ''')
            required_companies.append([company['symbol'],company['exchange'],company['exchangeShortName']
                                       ,specific_stock[0]['country']])

            required_companies_symbol.append(company['symbol'])
    response_shares_float=requests.get(f'https://fmpcloud.io/api/v4/shares_float/all?apikey={api_key}')
    data_shares_float=response_shares_float.json()

    conn=database_connection()


    cur = conn.cursor()
    insert_sql = '''
        INSERT INTO share_float (hashed_key,symbol,date,free_float,float_shares,outstanding_shares,exchange,stock_exchange,country)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (hashed_key) DO UPDATE SET
        (symbol,date,free_float,float_shares,outstanding_shares,exchange,stock_exchange,country) =
        (EXCLUDED.symbol,EXCLUDED.date,EXCLUDED.free_float,EXCLUDED.float_shares,EXCLUDED.outstanding_shares,EXCLUDED.exchange,EXCLUDED.stock_exchange,EXCLUDED.country);
    '''
    records=0
    print('Started Scraping and Storing data in database.')
    for company in data_shares_float:
        if (company['symbol'] in required_companies_symbol):
            try:
                records=records+1
                required_companies_symbol_index=required_companies_symbol.index(company['symbol'])
                hash_key=hashlib.sha256(company['symbol'].encode('utf-8')).hexdigest()
                insert_data=[hash_key,company['symbol'],company['date'],company["freeFloat"]
                             ,company['floatShares'],company["outstandingShares"]
                             ,required_companies[required_companies_symbol_index][1]
                             ,required_companies[required_companies_symbol_index][2]
                             ,required_companies[required_companies_symbol_index][3]]
                insert_data=replace_values(insert_data, '', None)
                cur.execute(insert_sql,tuple(insert_data))
                conn.commit()
            except Exception as e:
                print(e)
                conn=database_connection()
                cur = conn.cursor()
                cur.execute(insert_sql,tuple(insert_data))
                conn.commit()


    send_mail(send_from='faghost6201@gmail.com',
        subject='Share Float Table Status',
        text=f'The share float table has been updated with the recent data! The number of records which are updated are {records}',
        send_to=['bayo.billing@gmail.com','owaisahmed142002@gmail.com','alikhanhamza434@gmail.com','faghost6201@gmail.com'],
        files=[])
    print('Completed!')
if DB_ENV_PROD==1:
    REMOTE_HOST = os.getenv('REMOTE_HOST')
    REMOTE_USERNAME = os.getenv('REMOTE_USERNAME')
    PKEY_PATH= './humble.pem'
    
    conn_params = {
    'database': os.getenv('CLOUD_DB_NAME'), 
    'user': os.getenv('CLOUD_DB_USER'), 
    'password': os.getenv('CLOUD_DB_PASSWORD'), 
    'host': os.getenv('CLOUD_DB_HOST'), 
    'port': int(os.getenv('CLOUD_DB_PORT'))
    }
    
    def open_ssh_tunnel(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            tunnel = SSHTunnelForwarder((REMOTE_HOST),
                ssh_pkey=PKEY_PATH,
                ssh_username=REMOTE_USERNAME,
                remote_bind_address=(conn_params['host'],int(conn_params['port'])),
                )
            tunnel.start()
#             conn_params['port'] = tunnel.local_bind_port

            result = func(*args, **kwargs)

            tunnel.stop()
            return result
        return wrapper
    
    @open_ssh_tunnel
    def query_make_table():
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS share_float (
                hashed_key text NOT NULL PRIMARY KEY,
                symbol VARCHAR(255),
                date TIMESTAMP,
                free_float float(24),
                float_shares float(24),
                outstanding_shares float(24),
                exchange VARCHAR(255),
                stock_exchange VARCHAR(255),
                country VARCHAR(255)
            ); ''')
        conn.commit()
        conn.close()
        print('Query Executed!')
        
    query_make_table()
    
    @open_ssh_tunnel
    def insert_burst_data(data):
        print('Started Inserting data into database')
        conn = psycopg2.connect(**conn_params)
        insert_sql = '''
            INSERT INTO share_float (hashed_key,symbol,date,free_float,float_shares,outstanding_shares,exchange,stock_exchange,country)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (hashed_key) DO UPDATE SET
            (symbol,date,free_float,float_shares,outstanding_shares,exchange,stock_exchange,country) =
            (EXCLUDED.symbol,EXCLUDED.date,EXCLUDED.free_float,EXCLUDED.float_shares,EXCLUDED.outstanding_shares,EXCLUDED.exchange,EXCLUDED.stock_exchange,EXCLUDED.country);
        '''
        cur = conn.cursor()
        for record in data:
            try:
                cur.execute(insert_sql,tuple(record))
                conn.commit()
            except:
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                cur.execute(insert_sql,tuple(record))
                conn.commit()
        conn.close()
        print('All the data has been inserted!')

 
        
    response_stock_list=requests.get(f'https://fmpcloud.io/api/v3/stock/list?apikey={api_key}')
    data_stock_list=response_stock_list.json()
    required_companies=[]
    required_companies_symbol=[]

    options = uc.ChromeOptions()
    options.headless=True
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = uc.Chrome(service=Service(ChromeDriverManager().install()),use_subprocess=True,options=options)
    driver.get('https://fmpcloud.io/api/v3')
    driver.implicitly_wait(30)


    for company in data_stock_list:
        if (company['exchangeShortName']=='NASDAQ') or (company['exchangeShortName']=='NYSE'):  
            specific_stock=driver.execute_script('''
                var datas
                await fetch("https://fmpcloud.io/api/v3/profile/'''+company["symbol"]+'''?apikey='''+api_key+'''", {
                "headers": {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1"
              },
              "referrerPolicy": "strict-origin-when-cross-origin",
              "body": null,
              "method": "GET",
              "mode": "cors",
              "credentials": "include"
            }).then((response) => response.json()).then((data)=>datas=data)
            return datas ''')
            required_companies.append([company['symbol'],company['exchange'],company['exchangeShortName']
                                       ,specific_stock[0]['country']])
            required_companies_symbol.append(company['symbol'])
            
    response_shares_float=requests.get(f'https://fmpcloud.io/api/v4/shares_float/all?apikey={api_key}')
    data_shares_float=response_shares_float.json()
    records=0
    all_data=[]
    print('Started Scraping and Storing data in database.')
    for company in data_shares_float:
        if (company['symbol'] in required_companies_symbol):
            try:
                records=records+1
                required_companies_symbol_index=required_companies_symbol.index(company['symbol'])
                hash_key=hashlib.sha256(company['symbol'].encode('utf-8')).hexdigest()
                insert_data=[hash_key,company['symbol'],company['date'],company["freeFloat"]
                             ,company['floatShares'],company["outstandingShares"]
                             ,required_companies[required_companies_symbol_index][1]
                             ,required_companies[required_companies_symbol_index][2]
                             ,required_companies[required_companies_symbol_index][3]]
                insert_data=replace_values(insert_data, '', None)
                all_data.append(insert_data)

            except Exception as e:
                print(e)
                all_data.append(insert_data)
    insert_burst_data(all_data)
    send_mail(send_from='faghost6201@gmail.com',
        subject='Share Float Table Status',
        text=f'The share float table has been updated with the recent data! The number of records which are updated are {records}',
        send_to=['bayo.billing@gmail.com','owaisahmed142002@gmail.com','alikhanhamza434@gmail.com','faghost6201@gmail.com'],
        files=[])
    
    print('Completed!')
