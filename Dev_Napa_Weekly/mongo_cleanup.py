import datetime as dt
from datetime import datetime
import pymongo

# Connect to the local instance of MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

def clean_mongo():
    # Create a list of MongoDB databases to delete because they are overage or do not conform to naming conventions
    outdated_db_lst = []

    # Create a list of default MongoDB databases to never delete
    reserved_db_lst = ['admin','config','local']

    # Set max database age (in days)
    variables_lst = []
    with open('config_file.txt','r') as config_file:
        for line in config_file:
            variables_lst.append(line.strip())
    maxage = int(variables_lst[3].split('=')[1])

    # Loop through the name of each database in MongoDB
    for db in client.list_databases():
        dbname = db['name']
        if dbname in reserved_db_lst:
            # Ignore the database if it is a default MongoDB database
            pass
        else:
            try:
                # Get the database date if the name follows the current date nameing convention (ex. "02Aug2021")
                dbage = (datetime.now() - dt.datetime.strptime(dbname,"%d%b%Y")).days
            except:
                try:
                    # Get the database date if the name follows the week number nameing convention (ex. "wk31_2021")
                    year = dbname[-4:]
                    week = dbname[2:4]
                    dbage = (datetime.now() - dt.datetime.strptime('{}-{}'.format(year,week) + '-1','%Y-%W-%w')).days
                except:
                    # Mark the database for deleteion if it does not follow naming convention
                    outdated_db_lst.append(dbname)
                    print('Database {} does not conform to naming convention, will be deleted'.format(dbname))
        if dbage > maxage:
            # Mark the database for deletion if it is over age
            outdated_db_lst.append(dbname)
            print('Database {} is over {} days old, will be deleted.'.format(dbname,maxage))

    # Drop all databases marked for deletion
    for db in outdated_db_lst:
        client.drop_database(db)
    print('The following databases were deleted:\n{}'.format(outdated_db_lst))

if __name__ == '__main__':
    # Set the max database age in config.txt, then run this script to manually delete old databases
    clean_mongo()