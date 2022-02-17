import os
import argparse
import warnings
from threading import Thread

from selenium.webdriver.chrome.options import Options

from crawler import WatchapediaRatingCrawler


def main():
	warnings.filterwarnings('ignore')
	parser = argparse.ArgumentParser()
	parser.add_argument('--n_iter', '-i', default=10)
	parser.add_argument('--max_contents', '-m', default=300)
	parser.add_argument('--n_thread', '-t', default=1)
	parser.add_argument('--users_dir', '-udir', default='./user_id.txt')
	parser.add_argument('--rating_dir', '-rdir',default='../data/watchapedia_ratings')
	parser.add_argument('--wishlist_dir', '-wdir', default='../data/watchapedia_wishlist')
	args = parser.parse_args()

	n_iter = int(args.n_iter) 
	max_contents = int(args.max_contents)
	n_thread = int(args.n_thread)
	users_dir = args.users_dir
	rating_dir = args.rating_dir
	wishlist_dir = args.wishlist_dir

	# webdriver option
	options = Options()
	options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 1
            })

	with open(users_dir, 'r') as f:
		users = f.readlines()	
	f.close()
	users = [u.replace('\n', '') for u in users]

	thread_list = []
	for i in range(n_thread):
		user_id = users[i]
		rating_dir += f'_{i + 1}'
		wishlist_dir += f'_{i + 1}'

		thread = Thread(
			target=execute_crawler, 
			name=f'thread{i:0>2}',
			args=(user_id, rating_dir, wishlist_dir, n_iter, max_contents, options)
			)
		thread_list.append(thread)
		thread_list[i].start()

def execute_crawler(user_id, rating_dir, wishlist_dir, n_iter, max_contents, options):
    crawler = WatchapediaRatingCrawler(init_user_id=user_id, 
							    	rating_dir=rating_dir,
							    	wishlist_dir=wishlist_dir,
							    	options=options)

    crawler.run_crawler(rating_dir=rating_dir, 
    				wishlist_dir=wishlist_dir, 
    				n_iter=n_iter, 
    				max_contents=max_contents)

    return crawler


if __name__ == '__main__':
	main()



