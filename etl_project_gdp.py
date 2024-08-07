import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import sqlite3

#define information
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
sql_connection = sqlite3.connect('./World_Economies.db')
text_path = "./etl_project_log.txt"

#extract the web page as text
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—'not in col[2]:
                dir = {'Country':col[0].a.contents[0],
                       'GDP_USD_millions':col[2].contents[0]}
                df1 = pd.DataFrame(dir, index = [0])
                df = pd.concat([df1,df],ignore_index=True)
    return df


def transform(df):
    GDP_list = df['GDP_USD_millions'].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df = df.rename(columns = {'GDP_USD_millions':'GDP_USD_billions'})
    df['GDP_USD_billions'] = GDP_list
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index =True)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False )

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message, text_path):
    time_stamp_format= '%Y-%h-%d--%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(time_stamp_format)
    with open (text_path, 'a') as f:
        f.write(timestamp + ':'+' '+ message + '\n')

log_progress('Preliminaries complete. Initiating ETL process', text_path)
df=extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process', text_path)
df = transform(df)

log_progress('Data transformation complete. Initiating loading process', text_path)
load_to_csv(df,csv_path)

log_progress('Data saved to CSV file', text_path)
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query', text_path)
query_statement = f"SELECT Country FROM {table_name} WHERE GDP_USD_billions>100 "
run_query(query_statement, sql_connection)


log_progress('Process Complete', text_path)

















