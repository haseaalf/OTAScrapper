from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import FirefoxOptions
from datetime import datetime
import sys
import config
import logging.config
import logging
import re
import time
import random
import json

class Translator(object):
    def __init__(self,month):
        firefox_profile_dict = {
            1 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofile4FVzog",
            2 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileaXbGb6",
            3 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileCG0iDg",
            4 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofilegALo8D",
            5 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileGU3uPD",
            6 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileLEI5T6",
            7 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileqzqxPL",
            8 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofilesE3y89",
            9 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofilesEYvmp",
            10 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileuV8Vpz",
            11 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofileY8Sffk",
            12 : r"C:\Users\MSI-GAMMING\Documents\COOLYEAH\.skripsi\Code\OTAScrapper\translator\firefox_profile\rust_mozprofilezBb4gH",

        }
        options = webdriver.FirefoxOptions()
        options.set_preference('intl.accept_languages', 'en-US')
        options.add_argument("--headless")
        options.add_argument("-profile")
        options.add_argument(firefox_profile_dict[month])
        self.driver = webdriver.Firefox(options=options)

    def translate_text(self, src, dest, text):
        JS_ADD_TEXT_TO_INPUT = """
            var elm = arguments[0], txt = arguments[1];
            elm.value += txt;
            elm.dispatchEvent(new Event('change'));
            """
        
        driver = self.driver
        url = f'https://translate.google.com/?sl={src}&tl={dest}&op=translate'
        driver.get(url)
        text_area = driver.find_element(By.XPATH, '//textarea[@aria-label="Source text"]')
        # driver.execute_script(JS_ADD_TEXT_TO_INPUT, text_area, text)
        text_area.send_keys(text)
        elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div/span[@lang]/span/span[text()]')))
        complete_result = [element.text for element in elements]
        complete_result = ' '.join(complete_result)
        # translated_text = driver.find_element(By.XPATH, '//div/span[@lang]/span/span').text
        return complete_result
    

import json
import os
import config
import mysql.connector

def init_db():
    return mysql.connector.connect(
            host = config.configSQLCrawler.HOST,
            user = config.configSQLCrawler.USER,
            passwd = config.configSQLCrawler.PASSWD,
            # database = config.configSQLCrawler.DATABASE,
            database = 'data_skripsi',
            port = config.configSQLCrawler.PORT,
            auth_plugin=config.configSQLCrawler.AUTH_PLUGIN
        )

def get_cursor(con):
    try:
        con.ping(reconnect=True, attempts=3, delay=5)
    except mysql.connector.Error as e:
        raise Exception('db error')
    return con.cursor(buffered=True)

trip_to_gtranslate_code = {
    'in' :'id',
    'en' : 'en',
    'de' : 'de',
    'nl' : 'nl',
    'fr' : 'fr',
    'es' : 'es',
    'ru' : 'ru',
    'cs' : 'cs',
    'pt' : 'pt',
    'it' : 'it',
    'da' : 'da',
    'zhTW' : 'zh-TW',
    'ko' : 'ko',
    'ja' : 'ja',
    'ar' : 'ar',
    'zhCN' : 'zh-CN',
    'sv' : 'sv',
    'no' : 'no',
    'pl' : 'pl',
    'el' : 'el',
    'sr' : 'sr',
    'tr' : 'tr',
    'th' : 'th',
    'vi' : 'vi'
}

month = int(sys.argv[1])

db = init_db()
cursor = get_cursor(db)

cursor.execute(f'SELECT id, lang, title, text FROM hotel_review_filtered WHERE lang="in" AND (translated_text is null OR translated_text="") AND MONTH(travel_date)={month}')
all_data = cursor.fetchall()
baris = len(all_data)

translator = Translator(month)
num_of_char = 0

with open('error_id.txt') as file:
    lines = [line.rstrip() for line in file]


processed = 0
for i, row in enumerate(all_data):
    if '\n' not in row[3] and str(row[0]) not in lines:
        try:
            translated_text = translator.translate_text('id', 'en', row[3])
            txt = f'Original: {row[3]}\nTranslated: {translated_text}\n'
            try:
                with open(f'log{month}.txt','a+', newline='', encoding='utf-8') as log_txt:
                    log_txt.write(txt)
            except:
                print('Cant write')
            cursor.execute('UPDATE hotel_review_filtered SET translated_text=%s WHERE id=%s',(translated_text, row[0]))
            db.commit()
            print(row[0],'\n',txt)
            processed += 1
            print(i+1,'/',baris, '| Processed: ', processed)
        except Exception as e:
            print(e,'\n ERROR')
            with open('error_id.txt', 'a+', newline='') as error_txt:
                error_txt.write(str(row[0]) + '\n')
    
        
