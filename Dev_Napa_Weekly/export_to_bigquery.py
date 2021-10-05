import os
import datetime as dt
import pymongo
from datetime import datetime
import google.cloud.bigquery as bigquery
from google.cloud.exceptions import NotFound

#set GOOGLE_APPLICATION_CREDENTIALS=C:\AAP\Key_AAP.json
bq_key = 'C:\Webscraping\BigQuery_Keys\Key_advance-auto-parts-268704.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = bq_key
client_bq = bigquery.Client()

def export_to_bq(wk_num):
    # Create the schema for the BigQuery table
    SCHEMA = [
        bigquery.SchemaField('aap_part_number', 'STRING'),
        bigquery.SchemaField('part_number', 'STRING'),
        bigquery.SchemaField('interchange_number', 'STRING'),
        bigquery.SchemaField('item_name', 'STRING'),
        bigquery.SchemaField('retail_price', 'STRING'),
        bigquery.SchemaField('store_id', 'STRING'),
        bigquery.SchemaField('store_id_num', 'STRING'),
        bigquery.SchemaField('status_home', 'STRING'),
        bigquery.SchemaField('status_store', 'STRING'),
        bigquery.SchemaField('promos', 'STRING'),
        bigquery.SchemaField('hierarchy_level_1', 'STRING'),
        bigquery.SchemaField('hierarchy_level_2', 'STRING'),
        bigquery.SchemaField('hierarchy_level_3', 'STRING'),
        bigquery.SchemaField('hierarchy_level_4', 'STRING'),
        bigquery.SchemaField('hierarchy_level_5', 'STRING'),
        bigquery.SchemaField('hierarchy_level_6', 'STRING'),
        bigquery.SchemaField('category_link', 'STRING'),
        bigquery.SchemaField('url', 'STRING'),
        bigquery.SchemaField('status_availability', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
        bigquery.SchemaField('napa_product_line', 'STRING'),
        bigquery.SchemaField('sale_price', 'STRING')
    ]

    # Set the BigQuery table name
    table_id = "advance-auto-parts-268704.Staging.NAPA_Weekly_" + wk_num

    # Check if the table already exists, if not create the table
    try:
        table = client_bq.get_table(table_id)
    except NotFound:
        table_ref = bigquery.Table(table_id, schema=SCHEMA)
        table = client_bq.create_table(table_ref)

    # Configure connection to MongoDB
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[wk_num]
    col_output = db['napa_weekly']

    res = col_output.find({})
    rows_to_insert = []

    # Export data from MongoDB to BigQuery
    for rec in res:
        rows_to_insert.append((
            str(rec['aap_part_number']) if 'aap_part_number' in rec else None,
            str(rec['part_number']) if 'part_number' in rec else None,
            str(rec['interchange_number']) if 'interchange_number' in rec else None,
            str(rec['item_name']) if 'item_name' in rec else None,
            str(rec['retail_price']) if 'retail_price' in rec else None,
            str(rec['store_id']) if 'store_id' in rec else None,
            str(rec['store_id_num']) if 'store_id_num' in rec else None,
            str(rec['status_home']) if 'status_home' in rec else None,
            str(rec['status_store']) if 'status_store' in rec else None,
            str(rec['promos']) if 'promos' in rec else None,
            str(rec['hierarchy_level_1']) if 'hierarchy_level_1' in rec else None,
            str(rec['hierarchy_level_2']) if 'hierarchy_level_2' in rec else None,
            str(rec['hierarchy_level_3']) if 'hierarchy_level_3' in rec else None,
            str(rec['hierarchy_level_4']) if 'hierarchy_level_4' in rec else None,
            str(rec['hierarchy_level_5']) if 'hierarchy_level_5' in rec else None,
            str(rec['hierarchy_level_6']) if 'hierarchy_level_6' in rec else None,
            str(rec['category_link']) if 'category_link' in rec else None,
            str(rec['url']) if 'url' in rec else None,
            str(rec['status_availability']) if 'status_availability' in rec else None,
            str(rec['category']) if 'category' in rec else None,
            str(rec['napa_product_line']) if 'napa_product_line' in rec else None,
            str(rec['sale_price']) if 'sale_price' in rec else None,
        ))
        if len(rows_to_insert) == 5000:
            errors = client_bq.insert_rows(table, rows_to_insert)
            if errors == []:
                print('Rows Inserted')
            else:
                print('Error')
            rows_to_insert = []
    if rows_to_insert:
        errors = client_bq.insert_rows(table, rows_to_insert)
        if errors == []:
            print('Rows Inserted')
        else:
            print('Error')

# Manually export to BigQuery
if __name__ == '__main__':
    try:
        wk_num = 'wk' + (datetime.now()-dt.timedelta(2)).strftime('%V')  + '_' + datetime.now().strftime('%Y')
        export_to_bq(wk_num)
        print('{} table successfully exported to BigQuery'.format(wk_num))
    except:
        print('Error attempting to export table')