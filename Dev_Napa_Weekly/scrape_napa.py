import datetime
import os
import subprocess
import re
import random
import time
import numpy as np
import pandas as pd
import progressbar
import pymongo
from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from google.oauth2 import service_account

# Configure connection to mongodb
from webdriver_manager.chrome import ChromeDriverManager

client = pymongo.MongoClient('mongodb://localhost:27017/')

# Configure connection to BigQuery
# bq_key = 'C:\Webscraping\BigQuery_Keys\Key_advance-auto-parts-268704.json'
# bq_credentials = service_account.Credentials.from_service_account_file(bq_key)

# Chrome Options
# driver_path = 'C:\Webscraping\Chromedriver_exe\chromedriver.exe'
chrome_options = Options()
#
# # Configure webdriver to use an instance of chrome that appears normal and will be difficult to detect as a bot
chrome_options.add_experimental_option("debuggerAddress", "localhost:9000")
#
# # Import part number data from BigQuey
# sql = 'select * from DB_AAP.napa_part_numbers_weekly'
# lts_urls = pd.read_gbq(sql,project_id='advance-auto-parts-268704',credentials=bq_credentials)
# lts_urls = lts_urls.drop_duplicates().sort_values(by=['NAPA_Part_Number'],axis=0,ignore_index=True)
# lts_urls = lts_urls.NAPA_Part_Number.unique()

# Function to create an instance of chrome that appears normal and will be difficult to detect as a bot
def start_chrome():
    # os.chdir('C:\Program Files\Google\Chrome\Application')
    # subprocess.Popen('chrome.exe --new-window https://www.napaonline.com --remote-debugging-port=9000')
    chrome_options = Options()

    # chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("debuggerAddress", "localhost:9000")

    # PROXY = "185.30.232.25:9999"
    # chrome_options.add_argument('--proxy-server=%s' % PROXY)

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)


    time.sleep(30)

    return driver

# Function to set the store using cookies
def set_store(driver, store_value):
    cookie = {'domain': 'www.napaonline.com',
        'httpOnly': True,
        'name': 'napa-store',
        'path': '/',
        'secure': False,
        'value': store_value}


    driver.get('https://www.napaonline.com')

    time.sleep(10)

    driver.delete_cookie('ak_bmsc')
    driver.add_cookie(cookie)
    driver.refresh()

# Function to check if the part number is already in MongoDB
# def find_part(aap_part_number, store_id, col_output):
#     res = col_output.find({'aap_part_number': str(aap_part_number), 'store_id': str(store_id)})
#     for rec in res:
#         return rec
#     return None

# Function to scrape the website
def scrape(store_id, store_id_num, curr_date, start=None, end=None):
    # global lts_urls
    #
    # if start or end:
    #     lts_urls = lts_urls[start:end]
    #
    # print('Scraping part numbers: \n {}'.format(lts_urls))

    # Curr Date
    db = client[curr_date]
    col_output = db['napa_weekly']

    # Start the chrome browser
    # start_chrome()
    chrome_options = Options()
    # PROXY = "185.30.232.25:9999"
    # chrome_options.add_argument('--proxy-server=%s' % PROXY)
    chrome_options.add_experimental_option("debuggerAddress", "localhost:9000")
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)
    # Set the store id using cookies
    set_store(driver, store_id)
    
    for i in range(0,1000):
        part_number = 'AC'
        # category_link = "https://www.napaonline.com/en/search?text=AC&q=Ntt%3DAC%26Ntk%3DP_PartNumber%26N%3D0&referer=v2&pageSize=30"
        category_link = 'https://www.napaonline.com/en/search?text={0:}&q=Ntt%3D{0:}%26Ntk%3DP_PartNumber%26N%3D0&referer=v2&pageSize=30'.format(part_number)

        # Check in MongoDB if the part number has already been scraped
        # if find_part(part_number, store_id, col_output):
        #     continue

        driver.get(category_link)

        time.sleep(random.randrange(15,20))

        # Restart the browser if cloudflare is triggered
        if len(driver.find_elements_by_xpath('//*[@id="cf-content"]/h1/span')) > 0:
            print('cloudflare was triggered')
            os.system("taskkill /im chrome.exe /f")
            start_chrome()
            driver = webdriver.Chrome()

        try:
            WebDriverWait(driver, 480).until(EC.presence_of_element_located((By.XPATH, '//geo-product-list-item')))
        except TimeoutException:
            pass

        # Get a list of results from the webpage
        id_xpath = '//geo-product-list-item'
        id_lst = []
        id_lst = [element.get_attribute('id') for element in driver.find_elements_by_xpath(id_xpath)]

        # Record the data from each result
        for id in id_lst:
            try:
                item = {}
                interchange_number_xpath = '//geo-product-list-item[@id="{}"]//div[@class="geo-pod-detail tablet-desktop"]/div[@class="geo-pod-interchange-info"]/div[@class="geo-pod-normal-text"]'.format(id)
                item_name_xpath = '//geo-product-list-item[@id="{}"]//div[@class="geo-pod-title geo-productpod-top"]'.format(id)
                retail_price_xpath = '//geo-product-list-item[@id="{}"]//div[@class="geo-pod-price-cost"]/div[1]'.format(id)
                hierarchy_level_1_xpath = '//li[1]/a/span[@itemprop="name"]'
                hierarchy_level_2_xpath = '//li[2]/a/span[@itemprop="name"]'
                promos_xpath = '//geo-product-list-item[@id="{}"]//geo-promotions'.format(id)
                status_home_xpath = '//geo-product-list-item[@id="{}"]//div[@class="geo-pickup-text"]/div'.format(id)
                status_store_xpath = '//geo-product-list-item[@id="{}"]//div[@class="geo-delivery-text"]/div'.format(id)
                url_xpath = '//geo-product-list-item[@id="{}"]//a[@class="geo-prod_pod_title"]'.format(id)

                try: item['interchange_number'] = driver.find_element_by_xpath(interchange_number_xpath).text.split(':')[1].strip()
                except: item['interchange_number'] = None
                try: item['item_name'] = driver.find_element_by_xpath(item_name_xpath).text
                except: item['item_name'] = None
                try: item['retail_price'] = driver.find_element_by_xpath(retail_price_xpath).text
                except: item['retail_price'] = None
                try: item['status_home'] = driver.find_element_by_xpath(status_home_xpath).text
                except: item['status_home'] = None
                try: item['status_store'] = driver.find_element_by_xpath(status_store_xpath).text
                except: item['status_store'] = None
                try: item['promos'] = driver.find_element_by_xpath(promos_xpath).text
                except: item['promos'] = None
                try: item['hierarchy_level_1'] = driver.find_element_by_xpath(hierarchy_level_1_xpath).text
                except: item['hierarchy_level_1'] = None
                try: item['hierarchy_level_2'] = driver.find_element_by_xpath(hierarchy_level_2_xpath).text
                except: item['hierarchy_level_2'] = None

                item['part_number'] = id.replace('_',' ')
                item['category_link'] = category_link
                item['aap_part_number'] = part_number
                item['store_id'] = store_id
                item['store_id_num'] = store_id_num
                item['status_availability'] = driver.get_cookie('napa-store')['value']
                item['url'] = driver.find_element_by_xpath(url_xpath).get_attribute('href')

                # Insert the data for the result into MongoDB
                print(item)
                col_output.insert_one(item)
            except:
                msg_txt = 'Could not locate items {}'.format(id)
                print(msg_txt)

    # Close the Chrome window
    os.system("taskkill /im chrome.exe /f")

scrape('21226','6','20210928')