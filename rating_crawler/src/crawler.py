import os
import re
import time
import warnings
import requests
import logging
from collections import defaultdict

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

import utils


global complete_userlist
complete_userlist = []

class WatchapediaRatingCrawler:
    """
    왓챠피디아 평점 크롤링
    - options : selenium chrome options
    - user_id : user id of starting point  
    """

    def __init__(self, init_user_id, rating_dir, wishlist_dir, options):
        self.logger = utils.set_crawler_logger(level='info')
        self.user_id = self.set_target_user(init_user_id)
        self.driver = webdriver.Chrome(chrome_options=options)
        
        self.ratings = self.init_watchapedia_dataset(rating_dir)
        self.wishes = self.init_watchapedia_dataset(wishlist_dir)

        self.url_content = 'https://pedia.watcha.com/ko-KR/contents/' # + contents_id
        complete_userlist = list(set(self.ratings['user_id']))

    def set_target_user(self, user_id):
        self.user_id = user_id
        self.url_user =  f'https://pedia.watcha.com/ko-KR/users/{user_id}'
        self.url_ratings = f'https://pedia.watcha.com/ko-KR/users/{user_id}/contents/movies/ratings'
        self.url_wishes = f'https://pedia.watcha.com/ko-KR/users/{user_id}/contents/movies/wishes'
        return user_id

    def init_watchapedia_dataset(self, file_dir):
    	data = defaultdict(list)
    	if os.path.isfile(file_dir):
    		df = pd.read_csv(file_dir, encoding='utf-8')
    		for col in df.columns:
    			data[col] = list(df.to_dict()[col].values())
    	return data

    def get_scroll_end_point(self, user_id):
        url = f'https://pedia.watcha.com/ko-KR/users/{user_id}'
        r = requests.get(url)
        user_soup = BeautifulSoup(r.text)
        n_rating = count_ratings(user_soup)
        n_wish = count_wish_list(user_soup)
        r.close()
        return n_rating, n_wish

    def get_next_user_id(self, current_contents_id, max_count=10):
    	# 다음 스크래핑할 user id
        while True:
            count = 0
            next_user = get_user_id(
            	self.url_content + np.random.choice(current_contents_id, 1)[0]
            	)
            if next_user != -1 and next_user not in complete_userlist:
                break
            elif count > max_count:
                next_user = get_user_id(self.url_content + np.random.choice(
                    list(set(self.ratings['content_id'])), 1)[0])
            else:
                count += 1
        return next_user

    def scrap_user_rating(self, url, n_rating):
        # 평점 리스트 크롤링
        self.driver.get(url)
        time.sleep(1.5)
        
        # 웹툰평가 홍보 팝업창 제거
        utils.delete_promotion_window(self.driver)
        
        # 스크롤 끝까지 내리기
        utils.scroll_to_last_content(
        	self.driver, 
        	xpath='//*[@id="root"]/div/div[1]/section/section/div[1]/section/div[1]/div/ul/li', 
        	n_content=n_rating
        )

        # 데이터 수집
        html = self.driver.page_source
        content_soup = BeautifulSoup(html, 'html.parser')
        contents_id = get_content_id(content_soup)
        contents_title = get_content_title(content_soup)
        user_ratings = get_content_ratings(content_soup)
        complete_userlist.append(self.user_id)
        return contents_id, contents_title, user_ratings

    def scrap_user_wishes(self, url, n_wish):
        # 위시리스트 크롤링
        time.sleep(1.5)
        self.driver.get(url)

        # 웹툰평가 홍보 팝업창 제거
        utils.delete_promotion_window(self.driver)
        # 스크롤 끝까지 내리기
        utils.scroll_to_last_content(
            self.driver, 
            xpath='//*[@id="root"]/div/div[1]/section/section/section/div/div/ul/li', 
            n_content=n_wish
        )
        # 데이터 수집
        html = self.driver.page_source
        content_soup = BeautifulSoup(html, 'html.parser')
        contents_id = get_content_id(content_soup)
        contents_title = get_content_title(content_soup)
        return contents_id, contents_title

    def run_crawler(self, rating_dir, wishlist_dir, n_iter=10, max_contents=2000):
        start_time = time.time()
        for i in range(1, n_iter + 1):
            user_start = time.time()
            # 목표 사용자 id 설정
            if i > 1:
                self.set_target_user(next_user)

            # 스크롤 마지막 지점 찾기
            
            try:
                n_rating, n_wish = self.get_scroll_end_point(self.user_id)
            except IndexError:
                self.logger.info(f'user_id: {self.user_id} - result: failed')
                next_user = self.get_next_user_id(contents_id, max_count=10)
                i -= 1
                continue

            if n_rating > max_contents:
                n_rating = max_contents
            if n_wish > max_contents:
                n_wish = max_contents
        
            # 사용자 평점 크롤링 및 적재
            contents_id, contents_title, user_ratings = self.scrap_user_rating(self.url_ratings, n_rating)
            self.ratings['user_id'] += [self.user_id for _ in range(n_rating)]
            self.ratings['content_id'] += contents_id[:n_rating]
            self.ratings['title'] += contents_title[:n_rating]
            self.ratings['rating'] += user_ratings[:n_rating]

            # 사용자 위시리스트 크롤링 및 적재
            if n_wish > 0:
                wishes_contents_id, wishes_title = self.scrap_user_wishes(self.url_wishes, n_wish)
                self.wishes['user_id'] += [self.user_id for _ in range(n_wish)][:n_wish]
                self.wishes['content_id'] += wishes_contents_id[:n_wish]
                self.wishes['title'] += wishes_title[:n_wish]

            # 다음 사용자 선정
            self.logger.info(f"user_id: {self.user_id} - result: complete - time: {round(time.time() - user_start, 2)}'s")
            next_user = self.get_next_user_id(contents_id)
            self.logger.info(f"user_id: {next_user} - result: next")

            # 데이터 저장
            utils.save_csv(data=self.ratings, file_dir=rating_dir) # save rating data
            utils.save_csv(data=self.wishes, file_dir=wishlist_dir) # save rating data
            if i % 100 == 0 and i > 0:
                time.sleep(np.random.random() * 10)

        print('왓챠피디아 평점 데이터 크롤링 완료...')
        print('소요 시간:', time.time() - start_time)
        # self.driver.close()


def count_wish_list(soup, class_name="css-kcevqh-CategoryArchivesWishedCount e19zkogf18"):
    # soup = BeautifulSoup(html)
    string = str(soup.find_all(class_=class_name)[0])
    string = re.findall('<strong>.+</strong>', string)[0]
    n_wish = int(re.findall('\d+', string)[0])
    return n_wish

def count_ratings(soup, class_name="css-7xoi89-CategoryArchivesRatedCount e19zkogf17"):
    # soup = BeautifulSoup(html)
    string = str(soup.find_all(class_=class_name)[0])
    string = re.findall('->.+<', string)[0]
    n_rating = int(re.findall('\d+', string)[0])
    return n_rating

def get_content_title(soup, class_name='css-8y23cj'):
    data = soup.find_all(class_=class_name)
    result = []
    for i in range(len(data)):
        title = data[i].find_all('a')[0].attrs['title']
        result.append(title)
    return result

def get_content_id(soup, class_name='css-8y23cj'):
    data = soup.find_all(class_=class_name)
    result = []
    for i in range(len(data)):
        href = data[i].find_all('a')[0].attrs['href']
        content_id = href.split('/')[-1]
        result.append(content_id)
    return result

def get_content_ratings(soup, class_name='css-m9i0qw'):
    data = soup.find_all(class_=class_name)
    result = []
    for txt in data:
        text = re.findall('★.+<', str(txt))[0]
        rating = float('.'.join(re.findall('\d', text)))
        result.append(rating)
    return result

def get_user_id(url_content, 
				class_name="css-1f9m1s4-StylelessLocalLink eovgsd01"):
	r = requests.get(url_content + '/comments')
	soup = BeautifulSoup(r.text)
	for i in range(3):
		try:
			user_id = soup.find_all(class_=class_name)[i].attrs['href'].split('/')[-1]
			if user_id not in complete_userlist:
				r.close()
				return user_id
			elif i == 2:
				r.close()
				return user_id
		except IndexError:
			r.close()
			return -1


