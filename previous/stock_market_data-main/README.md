# Stock Market Data Project

This projects retrieves stock market data from an open-source api and stores it in postgres tables

#
## How to run the project:
* create a conda environment and activate it
```
conda create -n stock_mkt_data python=3.10
conda activate stock_mkt_data
```
* install requirements
```
pip install -r requirements.txt
```
* run the individual files in fundamental tables dir
```
python fundamental_tables.py
python general_script_1_v2.py
python price_volume_tables_v2.py
```

#
## import statement for financial data dir
```
sys.path.insert(0,'/home/ubuntu/stock_market_data')
```

#
## approx time taken for each script, in desc order 
* fundamental_tables ~  60 mins each; 18 hours total
* price_volume_tables ~ 60 mins
* general_script_1 ~ 60 mins
* general_script_2 ~ 2 secs

#
## cron commands
* fundamental - sunday 1 am
* price_vol - daily 1 am
* gen script 1 - daily 3 am
* gen script 2 - daily 3:30 am
```
00 01 * * * python3 /home/ubuntu/stock_market_data/financial_data/price_volume_tables_v2.py > /home/ubuntu/stock_market_data/logs/price_volume_tables_v2.log  2>&1

00 01 * * 0 python3 /home/ubuntu/stock_market_data/financial_data/fundamental_tables.py > /home/ubuntu/stock_market_data/logs/fundamental_tables.log  2>&1

00 03 * * * python3 /home/ubuntu/stock_market_data/financial_data/general_script_2_v1.py > /home/ubuntu/stock_market_data/logs/general_script_2_v1.log  2>&1

30 03 * * * python3 /home/ubuntu/stock_market_data/financial_data/general_script_1_v2.py > /home/ubuntu/stock_market_data/logs/general_script_1_v2.log  2>&1
```
* for runnning in background, locally:
```
python3 financial_data/fundamental_tables.py > logs/fundamental_tables.log  2>&1
python3 financial_data/price_volume_tables_v2.py > logs/price_volume_tables_v2.log  2>&1
python3 financial_data/general_script_1_v2.py > logs/general_script_1_v2.log  2>&1
python3 financial_data/general_script_2_v1.py > logs/general_script_2_v1.log  2>&1
```

#
## Connecting to ec2/lightsale instance
1. Download .pem key and using the terminal, cd to the dir where key is stored (say ~/.ssh/stock-mkt-key.pem)
```
cd ~/.ssh/stock-mkt-key.pem
```
2. chmod the key and run the below ssh command to access console.
```
chmod 400 stock-mkt-key.pem
ssh -i "~/.ssh/stock-mkt-key.pem" ubuntu@52.70.236.175
```
3. once in the console, cd to stock_market_data/financial_data to view the python scripts to run
```
cd ~/stock_market_data/financial_data
```

#
## Access database
* access database by running below command in terminal:
```
psql postgresql://stock_mkt_user:stock_mkt_pass@localhost:5432/stock_mkt_db
```
* run queries:
```
select * from company_profile limit 10;
```

#
## script versions:
* general_script_1_v2 processes tables using multiprocessing, unlike simple iteration of each table in general_script_1_v1
* price_volume_tables_1_v2 processes tables using multiprocessing, unlike simple iteration of each table in price_volume_tables_1_v1
* symbols_exchange_v2 returns a list of symbols whose records are not yet saved in the database and then appends the rest of the symbols, unlike symbols_exchange_v1 which passes symbols in the same order every time
* for given 1cpu vm, pool set to 3 workers
