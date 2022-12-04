import requests, datetime, psycopg2, random, os
from itertools import islice
from datetime import date, timedelta
from dotenv import load_dotenv

# theme_name_list = sg.theme_list()
# today = str(date.today()).split('-')

# while True:
#     sg.theme(theme_name_list[random.randint(0, len(theme_name_list)-1)])

#     layout=[[sg.Text('Enter the Starting date',size=(20, 1), font='Ubuntu',justification='left')],
#             [sg.Input(key='from', size=(20,1)), sg.CalendarButton('Calendar',font="Ubuntu",  target='from', default_date_m_d_y=(int(today[1]),int(today[2]),int(today[0])), )],
#             [sg.Text('Enter the Ending date',size=(20, 1), font='Ubuntu',justification='left')],
#             [sg.Input(key='to', size=(20,1)), sg.CalendarButton('Calendar',font="Ubuntu",  target='to', default_date_m_d_y=(int(today[1]),int(today[2]),int(today[0])), )],    
#             [sg.Button('OK', font=('Ubuntu',12)),sg.Button('CANCEL', font=('Ubuntu',12))]]

#     win =sg.Window('NYSE NASDAQ', layout)

#     e,v=win.read()
#     if e == None or e == "CANCEL":
#         starting_date_entry=None
#         ending_date_entry=None
#         win.close()
#         con = True
#         break
#     else:
#         starting_date_entry, ending_date_entry = v['from'].split(' ')[0], v['to'].split(' ')[0]
#         ending_date_entry = datetime.datetime.strptime(ending_date_entry, '%Y-%m-%d').date()
#         starting_date_entry = datetime.datetime.strptime(starting_date_entry, '%Y-%m-%d').date()
#         win.close()
#         break
         
# if starting_date_entry==None or ending_date_entry==None:
#     raise Exception('Please select the starting and ending date to start the script')

#     date_list = [starting_date_entry + timedelta(days=i) for i in range((ending_date_entry - starting_date_entry).days + 1)]
date_list=[date.today()]
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
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        return conn

    conn = database_connection()
    cur = conn.cursor()

elif DB_ENV_PROD==1:    
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
    def database_connection():
        conn = psycopg2.connect(**conn_params)
        return conn
    conn=database_connection()
    cur = conn.cursor()

def unique_columns(x):
    if x.lower().strip().replace('-', '_') in symbols_in_DB:
        return False
    else:
        return True

def clean_columns(x):
    if x.lower().strip() in cols:
        return True
    else:
        return False

def clean(x):
    if x['symbol'] in nyse_nasdaq_symbols:
        return True
    else:
        return False

types = ['open', 'low', 'high', 'close', 'volume']

url = f'https://fmpcloud.io/api/v3/stock/list?apikey={api_key}'
response = requests.get(url).json()

# NYSE NASDAQ FILTERATION

nyse_nasdaq = [i for i in response if 'NASDAQ' in i['exchangeShortName'] or 'NYSE' in i['exchangeShortName']]

nyse_nasdaq_symbols = [i['symbol'] for i in nyse_nasdaq]

nyse_nasdaq_symbols_cols = []

for val in nyse_nasdaq_symbols:
    if (val == 'ON' or val == 'TRUE' or val == 'ALL' or val == 'ANY' or val == 'ASC' or val == 'DO' or val == 'USER' or val == 'IS' or val == 'OR' or val == 'ELSE' or val == 'CMAX' or val == 'FOR'):
        nyse_nasdaq_symbols_cols.append(val+'_')
    else:
        nyse_nasdaq_symbols_cols.append(val)

length_to_split = [1599 for i in range(len(nyse_nasdaq_symbols_cols)//1599+1)]

Inputt = iter(nyse_nasdaq_symbols_cols)
Output = [list(islice(Inputt, elem)) for elem in length_to_split]

Output = [[f'{j} float(24)' for j in i] for i in Output]

for i in Output:
    i.insert(0, 'date TIMESTAMP NOT NULL PRIMARY KEY')

Output = list((tuple(item) for item in Output))
Output.sort(key=len, reverse=True)

for attr in types:
    # CREATE TABLE
    try:  
        for ind, elem in enumerate(Output):
            command=f'''
            CREATE TABLE IF NOT EXISTS stock_{attr}_prices_v{ind+1} {str(elem).replace("'", '').replace('-', '_')};
            '''
            try:
                cur.execute(command)
                conn.commit()
            except:
                conn.close()
                conn=database_connection()
                cur = conn.cursor()
                cur.execute(command)
                conn.commit()

    except Exception as e:
        print(e)

    ######################################################################################################################
    # INITIAL CHECKING FOR ALL SYMBOLS IN DB

    symbols_in_DB = []

    for i in range(7):
        query = f'''
        SELECT * FROM information_schema.columns WHERE table_name = 'stock_{attr}_prices_v{i+1}';
        '''
        try:
            cur.execute(query)
        except:
            conn.close()
            conn=database_connection()
            cur = conn.cursor()
            cur.execute(query)
        lst=cur.fetchall()
        symbols_in_DB += [i[3] for i in lst]

    columns_not_in_DB = list(filter(unique_columns, nyse_nasdaq_symbols_cols))

    if len(columns_not_in_DB) != 0:
        q=[f'ADD COLUMN {i} float(24)' for i in columns_not_in_DB]
        q[0] = f'ALTER TABLE stock_{attr}_prices_v7 {q[0]}'
        alter_query = str(q).replace('[', '').replace("'", '').replace(']', ';')
        try:
            cur.execute(alter_query)
            conn.commit()
        except:
            conn.close()
            conn=database_connection()
            cur = conn.cursor()
            cur.execute(alter_query)
            conn.commit()

######################################################################################################################

for attr in types:
    for date in date_list:
        try:
            url = f'https://fmpcloud.io/api/v3/batch-request-end-of-day-prices?date={str(date)}&apikey={api_key}'
            response = requests.get(url).json()
            if len(response) == 0:
                print(f'{attr} {date} gives no results')
                continue

            # CLEANING TO INSERT
            lst = list(filter(clean, response))

            for index,item in enumerate(lst):
                lst[index]['symbol'] = f" {item['symbol']} "

            for index,item in enumerate(lst):
                lst[index]['symbol'] = item['symbol'].replace(' ON ', 'ON_').replace(' TRUE ', 'TRUE_').replace(' ALL ', 'ALL_').replace(' ANY ', 'ANY_').replace(' ASC ', 'ASC_').replace(' DO ', 'DO_').replace(' USER ', 'USER_').replace(' IS ', 'IS_').replace(' OR ', 'OR_').replace(' ELSE ', 'ELSE_').replace(' OFF ', 'OFF_').replace(' CMAX ', 'CMAX_').replace('-', '_')

            attr_type = {}
            for i in lst:
                attr_type[f"{i['symbol']}"] = i[attr]

            attr_type_data = {key.strip(): val for (key, val) in attr_type.items()}
            lst2=list(attr_type.keys())

            # INSERTING INTO DATABASE
            for i in range(7):
                query = f'''
                SELECT * FROM information_schema.columns WHERE table_name = 'stock_{attr}_prices_v{i+1}';
                '''
                cur.execute(query)
                lst=cur.fetchall()
                cols = tuple([i[3] for i in lst])

                clean_cols = list(filter(clean_columns, lst2))
                if len(clean_cols) == 0:
                    continue

                clean_cols.insert(0, 'date')
                columns = tuple([i.strip().lower() for i in clean_cols])

                insert_data = [attr_type_data[i.upper()] for i in columns[1:]]
                insert_data.insert(0, str(date))
                insert_data = tuple(insert_data)

                columns_excluded = str([f'EXCLUDED.{i}' for i in columns][1:]).replace('[', '(').replace(']', ')').replace("'", '')
                value_tuple = tuple(['%s' for i in range(len(columns))])
                columns_dateless = str(list(columns[1:])).replace('[', '(').replace(']', ')').replace("'", '')

                try:
                    insert_query = f'''
                        INSERT INTO stock_{attr}_prices_v{i+1} {str(columns).replace("'", '')}
                        VALUES {str(value_tuple).replace("'", '')}
                        ON CONFLICT (date) DO UPDATE SET
                        {columns_dateless} = {columns_excluded};
                    '''
                    try:
                        cur.execute(insert_query, insert_data)
                        conn.commit()
                    except:
                        conn.close()
                        conn=database_connection()
                        cur = conn.cursor()
                        cur.execute(insert_query, insert_data)
                        conn.commit()
                except:
                    insert_query = f'''
                    INSERT INTO stock_{attr}_prices_v{i+1} {str(columns).replace("'", '')}
                    VALUES {str(value_tuple).replace("'", '')}
                    ON CONFLICT (date) DO UPDATE SET
                    {columns_dateless.replace('(', '').replace(')', '')} = {columns_excluded.replace('(', '').replace(')', '')};
                '''
                try:
                    cur.execute(insert_query, insert_data)
                    conn.commit()
                except:
                    conn.close()
                    conn=database_connection()
                    cur = conn.cursor()
                    cur.execute(insert_query, insert_data)
                    conn.commit()

            print(f'{attr} {date} <== ADDED TO DATABASE')
        except Exception as e:
            print(e)

cur.close()
