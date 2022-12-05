import requests
import sys
import json
from dotenv import load_dotenv
import os
import re
import pandas as pd
from functools import wraps
import time
# from sshtunnel import SSHTunnelForwarder
import psycopg2
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


def send_mail(send_from: str,subject: str,text: str,send_to: list,files=None):
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
    port=int(os.getenv('DB_PORT'))
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
    CREATE TABLE IF NOT EXISTS company_profile (
                hashed_key text NOT NULL PRIMARY KEY,
                symbol VARCHAR(255),
                price float(24),
                beta float(24),
                vol_avg float(24),
                mkt_Cap float(24),
                last_div float(24),
                range VARCHAR(255),
                changes float(24),
                company_name VARCHAR(255),
                currency VARCHAR(255),
                cik float(24),
                isin VARCHAR(255),
                cusip VARCHAR(255),
                exchange VARCHAR(255),
                exchange_short_name VARCHAR(255),
                industry VARCHAR(255),
                website VARCHAR(255),
                description text,
                ceo VARCHAR(255),
                sector VARCHAR(255),
                country VARCHAR(255),
                full_time_employees VARCHAR(255),
                phone VARCHAR(255),
                address VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                zip VARCHAR(255),
                dcf_diff float(24),
                dcf float(24),
                image VARCHAR(255),
                ipo_date TIMESTAMP,
                default_image BOOLEAN,
                is_etf BOOLEAN,
                is_actively_trading BOOLEAN,
                is_adr BOOLEAN,
                is_fund BOOLEAN
            );
    '''
    conn=database_connection()
    cur = conn.cursor()
    cur.execute(make_table_query)
    cur.close()
    conn.commit()
    response=requests.get(f'https://fmpcloud.io/api/v3/stock/list?apikey={api_key}', headers={'Content-Type': 'application/json'})
    data=response.json()



#     options = uc.ChromeOptions()
#     options.headless=True
#     options.add_argument('--headless')
#     options.add_argument('--disable-gpu')

#     driver = uc.Chrome(executable_path=ChromeDriverManager().install(),use_subprocess=True,options=options)
#     driver.get('https://fmpcloud.io/api/v3')
#     driver.implicitly_wait(30)


    conn=database_connection()

    cur = conn.cursor()
    insert_sql = '''
        INSERT INTO company_profile (hashed_key,address, beta, ceo, changes, cik, city, company_name, country, currency, cusip, dcf, dcf_diff, default_image, description, exchange, exchange_short_name, full_time_employees, image, industry, ipo_date, is_actively_trading, is_adr, is_etf, is_fund, isin, last_div, mkt_cap, phone, price, range, sector, state, symbol, vol_avg, website, zip)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (hashed_key) DO UPDATE SET
        (address, beta, ceo, changes, cik, city, company_name, country, currency, cusip, dcf, dcf_diff, default_image, description, exchange, exchange_short_name, full_time_employees, image, industry, ipo_date, is_actively_trading, is_adr, is_etf, is_fund, isin, last_div, mkt_cap, phone, price, range, sector, state, symbol, vol_avg, website, zip) = (EXCLUDED.address, EXCLUDED.beta, EXCLUDED.ceo, EXCLUDED.changes, EXCLUDED.cik, EXCLUDED.city, EXCLUDED.company_name, EXCLUDED.country, EXCLUDED.currency, EXCLUDED.cusip, EXCLUDED.dcf, EXCLUDED.dcf_diff, EXCLUDED.default_image, EXCLUDED.description, EXCLUDED.exchange, EXCLUDED.exchange_short_name, EXCLUDED.full_time_employees, EXCLUDED.image, EXCLUDED.industry, EXCLUDED.ipo_date, EXCLUDED.is_actively_trading, EXCLUDED.is_adr, EXCLUDED.is_etf, EXCLUDED.is_fund, EXCLUDED.isin, EXCLUDED.last_div, EXCLUDED.mkt_cap, EXCLUDED.phone, EXCLUDED.price, EXCLUDED.range, EXCLUDED.sector, EXCLUDED.state, EXCLUDED.symbol, EXCLUDED.vol_avg, EXCLUDED.website, EXCLUDED.zip);
    '''
    print('Started Scraping and Storing data in database.')
    for company in data:
        try:
            if (data.index(company)%1000==0 and data.index(company)!=0) or (data.index(company)+1==len(data)):
                print('Sending details to emails!')
                if (data.index(company)+1==len(data)):
                    message=f'Company Profile table has been updated! Total records in table are {len(data)}'
                else:
                    message=f'{data.index(company)} records have been updated in the Company Profile table.'
                send_mail(send_from='faghost6201@gmail.com',
                    subject='Company Profile Table Status',
                    text=message,
                    send_to=['bayo.billing@gmail.com','owaisahmed142002@gmail.com','alikhanhamza434@gmail.com','faghost6201@gmail.com'],
                    files=[])
            specific_stock=requests.get(f'https://fmpcloud.io/api/v3/profile/{company["symbol"]}?apikey={api_key}', headers={'Content-Type': 'application/json'}).json()
#             specific_stock=driver.execute_script('''
#                 var datas
#                 await fetch("https://fmpcloud.io/api/v3/profile/'''+company["symbol"]+'''?apikey='''+api_key+'''", {
#                 "headers": {
#                 "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
#                 "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
#                 "cache-control": "no-cache",
#                 "pragma": "no-cache",
#                 "sec-fetch-dest": "document",
#                 "sec-fetch-mode": "navigate",
#                 "sec-fetch-site": "none",
#                 "sec-fetch-user": "?1",
#                 "upgrade-insecure-requests": "1"
#               },
#               "referrerPolicy": "strict-origin-when-cross-origin",
#               "body": null,
#               "method": "GET",
#               "mode": "cors",
#               "credentials": "include"
#             }).then((response) => response.json()).then((data)=>datas=data)
#             return datas ''')

            specific_stock_data=list(specific_stock[0].values())        
            hash_string=specific_stock[0]['symbol']
            hashed_key = hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
            specific_stock_data.insert(0,hashed_key)
            specific_stock_data = replace_values(specific_stock_data, '', None)
            cur.execute(insert_sql,tuple(specific_stock_data))
            conn.commit()
        except Exception as e:
            print(e)
            print(data.index(company))
            conn=database_connection()
            cur = conn.cursor()
            cur.execute(insert_sql,tuple(specific_stock_data))
            conn.commit()
    print('Completed!')
if DB_ENV_PROD==1:    
    REMOTE_HOST = os.getenv('REMOTE_HOST')
    REMOTE_USERNAME = os.getenv('REMOTE_USERNAME')
    # PKEY_PATH= os.environ.get('PKEY_PATH')
    
    conn_params = {
    'database': os.getenv('CLOUD_DB_NAME'), 
    'user': os.getenv('CLOUD_DB_USER'), 
    'password': os.getenv('CLOUD_DB_PASSWORD'), 
    'host': os.getenv('CLOUD_DB_HOST'), 
    'port': int(os.getenv('CLOUD_DB_PORT'))
    }
#     def open_ssh_tunnel(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):

#             tunnel = SSHTunnelForwarder((REMOTE_HOST),
#                 ssh_pkey=PKEY_PATH,
#                 ssh_username=REMOTE_USERNAME,
#                 remote_bind_address=(conn_params['host'],int(conn_params['port'])),
#                 )
#             tunnel.start()
# #             conn_params['port'] = tunnel.local_bind_port

#             result = func(*args, **kwargs)

#             tunnel.stop()
#             return result
#         return wrapper

#     @open_ssh_tunnel
    def query_make_table():
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS company_profile (
        hashed_key text NOT NULL PRIMARY KEY,
        symbol VARCHAR(255),
        price float(24),
        beta float(24),
        vol_avg float(24),
        mkt_Cap float(24),
        last_div float(24),
        range VARCHAR(255),
        changes float(24),
        company_name VARCHAR(255),
        currency VARCHAR(255),
        cik float(24),
        isin VARCHAR(255),
        cusip VARCHAR(255),
        exchange VARCHAR(255),
        exchange_short_name VARCHAR(255),
        industry VARCHAR(255),
        website VARCHAR(255),
        description text,
        ceo VARCHAR(255),
        sector VARCHAR(255),
        country VARCHAR(255),
        full_time_employees VARCHAR(255),
        phone VARCHAR(255),
        address VARCHAR(255),
        city VARCHAR(255),
        state VARCHAR(255),
        zip VARCHAR(255),
        dcf_diff float(24),
        dcf float(24),
        image VARCHAR(255),
        ipo_date TIMESTAMP,
        default_image BOOLEAN,
        is_etf BOOLEAN,
        is_actively_trading BOOLEAN,
        is_adr BOOLEAN,
        is_fund BOOLEAN
        ); ''')
        conn.commit()
        conn.close()
        print('Query Executed!')
    
    query_make_table()
#     @open_ssh_tunnel
    def insert_burst_data(data):
        print('Started Inserting data into database')
        conn = psycopg2.connect(**conn_params)
        insert_sql = '''
            INSERT INTO company_profile (hashed_key,address, beta, ceo, changes, cik, city, company_name, country, currency, cusip, dcf, dcf_diff, default_image, description, exchange, exchange_short_name, full_time_employees, image, industry, ipo_date, is_actively_trading, is_adr, is_etf, is_fund, isin, last_div, mkt_cap, phone, price, range, sector, state, symbol, vol_avg, website, zip)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (hashed_key) DO UPDATE SET
            (address, beta, ceo, changes, cik, city, company_name, country, currency, cusip, dcf, dcf_diff, default_image, description, exchange, exchange_short_name, full_time_employees, image, industry, ipo_date, is_actively_trading, is_adr, is_etf, is_fund, isin, last_div, mkt_cap, phone, price, range, sector, state, symbol, vol_avg, website, zip) = (EXCLUDED.address, EXCLUDED.beta, EXCLUDED.ceo, EXCLUDED.changes, EXCLUDED.cik, EXCLUDED.city, EXCLUDED.company_name, EXCLUDED.country, EXCLUDED.currency, EXCLUDED.cusip, EXCLUDED.dcf, EXCLUDED.dcf_diff, EXCLUDED.default_image, EXCLUDED.description, EXCLUDED.exchange, EXCLUDED.exchange_short_name, EXCLUDED.full_time_employees, EXCLUDED.image, EXCLUDED.industry, EXCLUDED.ipo_date, EXCLUDED.is_actively_trading, EXCLUDED.is_adr, EXCLUDED.is_etf, EXCLUDED.is_fund, EXCLUDED.isin, EXCLUDED.last_div, EXCLUDED.mkt_cap, EXCLUDED.phone, EXCLUDED.price, EXCLUDED.range, EXCLUDED.sector, EXCLUDED.state, EXCLUDED.symbol, EXCLUDED.vol_avg, EXCLUDED.website, EXCLUDED.zip);
        '''
        cur = conn.cursor()
        for record in data:
            if (data.index(record)%1000==0 and data.index(record)!=0) or (data.index(record)+1==len(data)):
                print('Sending details to emails!')
                if (data.index(record)+1==len(data)):
                    message=f'Company Profile table has been updated! Total records in table are {len(data)}'
                else:
                    message=f'{data.index(record)} records have been updated in the Company Profile table.'
                send_mail(send_from='faghost6201@gmail.com',
                    subject='Company Profile Table Status',
                    text=message,
                    send_to=['bayo.billing@gmail.com','owaisahmed142002@gmail.com','alikhanhamza434@gmail.com','faghost6201@gmail.com'],
                    files=[])
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

    all_data=[]
    response=requests.get(f'https://fmpcloud.io/api/v3/stock/list?apikey={api_key}', headers={'Content-Type': 'application/json'})
    data=response.json()
#     options = uc.ChromeOptions()
#     options.headless=True
#     options.add_argument('--headless')
#     options.add_argument('--disable-gpu')

#     driver = uc.Chrome(executable_path=ChromeDriverManager().install(),use_subprocess=True,options=options)
#     driver.get('https://fmpcloud.io/api/v3')
#     driver.implicitly_wait(30)
    print('Started Scraping the data!')
    for company in data:
        try:
            specific_stock=requests.get(f'https://fmpcloud.io/api/v3/profile/{company["symbol"]}?apikey={api_key}', headers={'Content-Type': 'application/json'}).json()
#             specific_stock=driver.execute_script('''
#                 var datas
#                 await fetch("https://fmpcloud.io/api/v3/profile/'''+company["symbol"]+'''?apikey='''+api_key+'''", {
#                 "headers": {
#                 "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
#                 "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
#                 "cache-control": "no-cache",
#                 "pragma": "no-cache",
#                 "sec-fetch-dest": "document",
#                 "sec-fetch-mode": "navigate",
#                 "sec-fetch-site": "none",
#                 "sec-fetch-user": "?1",
#                 "upgrade-insecure-requests": "1"
#               },
#               "referrerPolicy": "strict-origin-when-cross-origin",
#               "body": null,
#               "method": "GET",
#               "mode": "cors",
#               "credentials": "include"
#             }).then((response) => response.json()).then((data)=>datas=data)
#             return datas ''')

            specific_stock_data=list(specific_stock[0].values())        
            hash_string=specific_stock[0]['symbol']
            hashed_key = hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
            specific_stock_data.insert(0,hashed_key)
            specific_stock_data = replace_values(specific_stock_data, '', None)
            all_data.append(specific_stock_data)

        except Exception as e:
            print(e)
            print(data.index(company))
            all_data.append(specific_stock_data)
            
    insert_burst_data(all_data)
    print('Completed Collecting Data!')



