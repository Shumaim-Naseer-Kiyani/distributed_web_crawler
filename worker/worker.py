from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
# Path to ChromeDriver (update if needed)
CHROMEDRIVER_PATH = 'e:/web_crawler/chromedriver-win64/chromedriver.exe'

# Fetch rendered HTML using Selenium (for Amazon URLs)
def fetch_html_selenium(url, timeout=15):
	options = Options()
	options.add_argument('--headless')
	options.add_argument('--disable-gpu')
	options.add_argument('--no-sandbox')
	options.add_argument('--window-size=1920,1080')
	service = Service(CHROMEDRIVER_PATH)
	try:
		driver = webdriver.Chrome(service=service, options=options)
		driver.set_page_load_timeout(timeout)
		driver.get(url)
		html = driver.page_source
		driver.quit()
		return html
	except WebDriverException as e:
		print(f"[ERROR] Selenium failed for {url}: {e}")
		return None

# Worker Process for Distributed Web Crawler
# Connects to Memurai (Redis) for task coordination
# Requirements: requests, beautifulsoup4, redis


import time
import redis
import requests
from bs4 import BeautifulSoup
import re
import json
import socket
import os

# Connect to Memurai (Redis) running locally
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


# Redis keys (must match master)
URL_QUEUE = 'crawler:queue'         # Redis List: distributed task queue
VISITED_SET = 'crawler:visited'     # Redis Set: track visited URLs
RESULTS_HASH = 'crawler:results'    # Redis Hash: store crawl results
WORKERS_HASH = 'crawler:workers'    # Redis Hash: track worker activity

# Generate a unique worker ID (hostname + pid)
WORKER_ID = f"{socket.gethostname()}_{os.getpid()}"

# Robust Amazon price extraction
def extract_amazon_price(soup):
	# Match the specific price container
	price_container = soup.find("span", class_="a-price aok-align-center reinventPricePriceToPayMargin priceToPay")
	if price_container:
		symbol = price_container.find("span", class_="a-price-symbol")
		whole = price_container.find("span", class_="a-price-whole")
		decimal = price_container.find("span", class_="a-price-decimal")
		fraction = price_container.find("span", class_="a-price-fraction")
		if symbol and whole and decimal and fraction:
			whole_text = whole.get_text("", strip=True).replace(decimal.get_text("", strip=True), "")
			price = f"{symbol.get_text(strip=True)}{whole_text}{decimal.get_text(strip=True)}{fraction.get_text(strip=True)}"
			print("[PRICE HTML] Amazon price container (prettified):\n", price_container.prettify())
			print("[PRICE HTML] Amazon price container (raw):\n", str(price_container))
			return price
		# Fallback: just get the whole price if available
		if whole:
			print("[PRICE HTML] Amazon price container (prettified):\n", price_container.prettify())
			print("[PRICE HTML] Amazon price container (raw):\n", str(price_container))
			return whole.get_text(strip=True)
	# If not found, fallback to previous logic (other price blocks)
	for pid in ["priceblock_ourprice", "priceblock_dealprice", "priceblock_saleprice"]:
		p = soup.find(id=pid)
		if p:
			print(f"[PRICE HTML] Amazon price block id={pid} (prettified):\n", p.prettify())
			print(f"[PRICE HTML] Amazon price block id={pid} (raw):\n", str(p))
			return p.get_text(strip=True)
	# Fallback to first a-price-whole
	p = soup.find("span", class_="a-price-whole")
	if p:
		print("[PRICE HTML] Amazon a-price-whole (prettified):\n", p.prettify())
		print("[PRICE HTML] Amazon a-price-whole (raw):\n", str(p))
		return p.get_text(strip=True)
	return None
	# Fallback to alternate price blocks
	for pid in ["priceblock_ourprice", "priceblock_dealprice", "priceblock_saleprice"]:
		p = soup.find(id=pid)
		if p:
			print(f"[PRICE HTML] Amazon price block id={pid} (prettified):\n", p.prettify())
			print(f"[PRICE HTML] Amazon price block id={pid} (raw):\n", str(p))
			return p.get_text(strip=True)
	# Fallback to first a-price-whole
	p = soup.find("span", class_="a-price-whole")
	if p:
		print("[PRICE HTML] Amazon a-price-whole (prettified):\n", p.prettify())
		print("[PRICE HTML] Amazon a-price-whole (raw):\n", str(p))
		return p.get_text(strip=True)
	return None



# Extract product data from HTML
def extract_product_data(html):
	soup = BeautifulSoup(html, 'html.parser')
	text = soup.get_text()
	# Amazon price extraction
	price = None
	# Try Amazon-specific extraction if URL is Amazon
	# (Assume caller passes Amazon HTML for Amazon URLs)
	price = extract_amazon_price(soup)
	if not price:
		# Fallback to regex
		price_match = re.search(r'(\$|USD)?\s?([0-9]+[.,][0-9]{2})', text)
		if price_match:
			# Try to find the HTML containing the price string
			price_str = price_match.group(0).strip()
			price_tag = soup.find(string=re.compile(re.escape(price_str)))
			if price_tag:
				parent = price_tag.parent if price_tag.parent else price_tag
				print("[PRICE HTML] Regex price match (prettified):\n", parent.prettify())
				print("[PRICE HTML] Regex price match (raw):\n", str(parent))
			else:
				print(f"[PRICE HTML] Regex price string found in text: {price_str}")
			price = price_str
		else:
			price = 'N/A'
	# Name (try common tags)
	name = soup.title.string.strip() if soup.title and soup.title.string else 'N/A'
	# Rating (look for common patterns)
	rating_match = re.search(r'([0-5]\.?[0-9]?)\s?out of\s?5', text)
	rating = rating_match.group(1) if rating_match else 'N/A'
	# Availability (look for 'In Stock' or 'Out of Stock')
	if 'in stock' in text.lower():
		availability = 'In Stock'
	elif 'out of stock' in text.lower():
		availability = 'Out of Stock'
	else:
		availability = 'N/A'
	# Image URL (first <img> tag)
	img_tag = soup.find('img')
	image_url = img_tag['src'] if img_tag and img_tag.has_attr('src') else 'N/A'
	# Description (first <meta name="description">)
	desc_tag = soup.find('meta', attrs={'name': 'description'})
	description = desc_tag['content'] if desc_tag and desc_tag.has_attr('content') else 'N/A'
	return {
		'name': name,
		'price': price,
		'rating': rating,
		'availability': availability,
		'image_url': image_url,
		'description': description
	}

def worker_loop():
	print("Worker started. Waiting for tasks...")
	# Register as idle on startup
	r.hset(WORKERS_HASH, WORKER_ID, "idle")
	while True:
		# Use BLPOP for blocking pop from the queue (atomic, safe for parallel workers)
		result = r.blpop(URL_QUEUE, timeout=10)  # (queue_name, task_json)
		if result is None:
			# Mark worker as idle
			r.hset(WORKERS_HASH, WORKER_ID, "idle")
			print("No tasks in queue. Worker sleeping...")
			time.sleep(5)
			continue
		_, task_json = result
		try:
			print(f"[DEBUG] Pulled task_json: {task_json}")
			task = json.loads(task_json)
			session_id = task.get('session_id')
			url = task.get('url')
			print(f"[DEBUG] Extracted url: type={type(url)}, value={url}")
			# Defensive: if url is a dict, extract 'url' key again
			if isinstance(url, dict) and 'url' in url:
				print(f"[DEBUG] url was a dict, extracting inner url: {url}")
				url = url['url']
			# If url is not a string, convert to string and warn
			if not isinstance(url, str):
				print(f"[DEBUG] url is not a string! type: {type(url)}, value: {url}")
				url = str(url)
		except Exception as e:
			print(f"Malformed task in queue: {task_json}")
			r.hset(WORKERS_HASH, WORKER_ID, "idle")
			continue
		# Register current task
		r.hset(WORKERS_HASH, WORKER_ID, url)
		# Check if URL already visited (atomic set add)
		if r.sismember(VISITED_SET, url):
			print(f"URL already visited: {url}")
			r.hset(WORKERS_HASH, WORKER_ID, "idle")
			continue
		# Mark as visited (atomic add)
		r.sadd(VISITED_SET, url)
		# Ensure url is a string
		if not isinstance(url, str):
			print(f"[DEBUG] url is not a string! type: {type(url)}, value: {url}")
			url = str(url)
		if isinstance(url, str) and url.startswith('http'):
			print(f"Crawling: {url}")
			session_results_hash = f"crawler:results:{session_id}"
			try:
				html = None
				# Use Selenium for Amazon URLs, requests for others
				if 'amazon.com' in url:
					html = fetch_html_selenium(url)
					if html is None:
						error_data = {'error': 'Selenium failed to fetch page'}
						r.hset(session_results_hash, url, json.dumps(error_data))
						print(f"Failed to fetch {url} with Selenium")
						continue
				else:
					resp = requests.get(url, timeout=10)
					if resp.status_code == 200:
						html = resp.text
					else:
						error_data = {'error': f'HTTP {resp.status_code}'}
						r.hset(session_results_hash, url, json.dumps(error_data))
						print(f"Failed to fetch {url}: HTTP {resp.status_code}")
						continue
				product_data = extract_product_data(html)
				# Store result as JSON in session-specific Redis hash (URL: JSON string)
				r.hset(session_results_hash, url, json.dumps(product_data))
				print(f"Extracted data: {product_data}")
			except Exception as e:
				error_data = {'error': str(e)}
				r.hset(session_results_hash, url, json.dumps(error_data))
				print(f"Error crawling {url}: {e} (URL: {url})")
		else:
			print(f"[ERROR] Skipping invalid url: type={type(url)}, value={url}")
		# Mark worker as idle after finishing task
		r.hset(WORKERS_HASH, WORKER_ID, "idle")

if __name__ == '__main__':
	worker_loop()

# Notes on Memurai (Redis) for safe parallel access:
# - BLPOP is atomic: multiple workers can safely pull unique URLs without conflict.
# - SADD and SISMEMBER are atomic: ensures each URL is only processed once.
# - HSET is atomic: results from different workers are safely stored without overwriting unrelated data.
