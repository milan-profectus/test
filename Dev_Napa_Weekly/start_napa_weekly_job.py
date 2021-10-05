import math
import datetime as dt
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from scrape_napa import scrape
from export_to_bigquery import export_to_bq
from send_mail import send_mail
from mongo_cleanup import clean_mongo

# Set Variables
variables_lst = []
with open('C:\Webscraping\webscrapping\Dev_Napa_Weekly\config_file.txt','r') as config_file:
    for line in config_file:
        variables_lst.append(line.strip())
        
machine_num = int(variables_lst[0].split('=')[1])
total_machines = int(variables_lst[1].split('=')[1])
number_of_chrome_windows = int(variables_lst[2].split('=')[1])
wk_num = 'wk' + (datetime.now()-dt.timedelta(2)).strftime('%V')  + '_' + datetime.now().strftime('%Y') # Name for mongodb database

# Configure logging
logging.basicConfig(filename='scraping_log.log', level=logging.WARNING, format='%(asctime)s:%(levelname)s:%(message)s')

# Configure connection to BigQuery
bq_key = 'C:\Webscraping\BigQuery_Keys\Key_advance-auto-parts-268704.json'
bq_credentials = service_account.Credentials.from_service_account_file(bq_key)

# Import store data from BigQuey
sql_storeids = 'select * from DB_AAP.napa_store_ids_weekly'
df_storeids = pd.read_gbq(sql_storeids,project_id='advance-auto-parts-268704',credentials=bq_credentials)
df_storeids = df_storeids.sort_values(by=['zipcode'],axis=0,ignore_index=True)

# Import part number data from BigQuey
sql_pn = 'select * from DB_AAP.napa_part_numbers_weekly'
df_pn = pd.read_gbq(sql_pn,project_id='advance-auto-parts-268704',credentials=bq_credentials)
df_pn = df_pn.drop_duplicates().sort_values(by=['NAPA_Part_Number'],axis=0,ignore_index=True)

# Import email receivers
sql_email = 'select * from DB_AAP.Webscraping_Email_Receivers'
df_email = pd.read_gbq(sql_email, project_id='advance-auto-parts-268704', credentials=bq_credentials)
email_receivers = df_email['emails'].tolist()

# Create list of store ids for this VM to scrape (the list is the indexes of the store_id dataframe)
store_num_lst = []
store_num = machine_num - 1
for i in range(math.ceil(len(df_storeids)/total_machines)):
    if store_num > len(df_storeids): break
    store_num_lst.append(store_num)
    store_num = store_num + total_machines

# Set range of part numbers to scrape
start_pn = 0
end_pn = len(df_pn)

print('Store numbers to scrape: {}'.format(store_num_lst))

try:
    if __name__ == '__main__':
        for store_num in store_num_lst:   

            # Log start of job
            timestamp = str(datetime.now().strftime('%c'))
            msg_txt = 'Started scraping id# {} @ {}'.format(store_num, timestamp)
            print(msg_txt)
            logging.warning(msg_txt)

            store_id = str(df_storeids['cookie_storeId'][store_num])
            store_id_num = int(df_storeids['store_id'][store_num])

            scrape(store_id, store_id_num, wk_num, start_pn, end_pn)
        
        # Export the data from MongoDB to BigQuery
        export_to_bq(wk_num)

        # Clean up old or non conforming databases in MonngoDB
        clean_mongo()

        # Log end of job and send notification email
        timestamp = str(datetime.now().strftime('%c'))
        msg_sbj = 'Completed Napa weekly job on machine# {} @ {}'.format(machine_num, timestamp)
        msg_txt = 'Completed Napa weekly job on machine# {} @ {}'.format(machine_num, timestamp)
        logging.warning(msg_txt)
        send_mail(email_receivers, msg_sbj, msg_txt)

# Send error if job fails while scraping
except:
    timestamp = str(datetime.now().strftime('%c'))
    msg_sbj = 'Napa weekly job failed while scraping data on machine# {} @ {}'.format(machine_num, timestamp)
    msg_txt = 'Napa weekly job failed while scraping data on machine# {} @ {}'.format(machine_num, timestamp)
    logging.warning(msg_txt)
    send_mail(email_receivers, msg_sbj, msg_txt)