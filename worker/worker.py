
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


# Extract product data from HTML
def extract_product_data(html):
	soup = BeautifulSoup(html, 'html.parser')
	text = soup.get_text()
	# Price
	price_match = re.search(r'(\$|USD)?\s?([0-9]+[.,][0-9]{2})', text)
	price = price_match.group(0).strip() if price_match else 'N/A'
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
				resp = requests.get(url, timeout=10)
				if resp.status_code == 200:
					product_data = extract_product_data(resp.text)
					# Store result as JSON in session-specific Redis hash (URL: JSON string)
					r.hset(session_results_hash, url, json.dumps(product_data))
					print(f"Extracted data: {product_data}")
				else:
					error_data = {'error': f'HTTP {resp.status_code}'}
					r.hset(session_results_hash, url, json.dumps(error_data))
					print(f"Failed to fetch {url}: HTTP {resp.status_code}")
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
