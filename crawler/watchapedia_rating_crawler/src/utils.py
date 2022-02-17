import os
import time
import random
import requests
import logging
from collections import defaultdict

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys


def create_dir(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory) 
    except OSError:
        print("Error: Failed to create the directory.")

def set_crawler_logger(directory='./log/', level='error'):
    logger = logging.getLogger()
    
    if level.lower() == 'debug':
        logger.setLevel(logging.DEBUG)
    elif level.lower() == 'info':
        logger.setLevel(logging.INFO)
    elif level.lower() == 'warning':
        logger.setLevel(logging.WARNING)
    elif level.lower() == 'error':
        logger.setLevel(logging.ERROR)
    elif level.lower() == 'critical':
        logger.setLevel(logging.CRITICAL)

    # log 출력 형식
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    # log 출력
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # log를 파일에 출력
    create_dir(directory)
    file_handler = logging.FileHandler(directory + 'crawler.log') # file 저장
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def save_csv(data, file_dir, index=False, verbose=False):
    if file_dir.endswith('.csv') == False:
        file_dir += '.csv'
    if isinstance(data, pd.DataFrame) == False:
    	data = pd.DataFrame(data)
    data.to_csv(file_dir, index=index, encoding='utf-8')
    if verbose:
    	print(f'{file_dir} saving done..')

def delete_promotion_window(webdriver, class_name='css-l5xf11'):
	while True:
		try:
			time.sleep(1)
			esc_button = webdriver.find_element_by_class_name(class_name)
			esc_button.click()
			break
		except:
			print("pop-up window loading... wait 10 seconds..")
			time.sleep(10)
			continue

def scroll_to_last_content(webdriver, n_content, xpath, verbose=False):
    #ActionChains생성
    action = ActionChains(webdriver)
    #리스트 가져오기
    while True:
        sleep_time = random.random() * 10
        if sleep_time > 0.5:
            sleep_time /= 10
        time.sleep(sleep_time)
        content_list = webdriver.find_elements_by_xpath(xpath)        
        try:
            #move_to_element를 이용하여 이동
            action.move_to_element(content_list[n_content - 1]).perform()
            break
        except IndexError:
            webdriver.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
    if verbose:
    	print(f'scroll to last {n_content}.')



 