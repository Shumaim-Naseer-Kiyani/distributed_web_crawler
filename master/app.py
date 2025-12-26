
from flask_cors import CORS
import json

# Flask Master Node for Distributed Web Crawler
# Uses Memurai (Redis) for task distribution and synchronization
# Requirements: Flask, redis-py

from flask import Flask, request, jsonify
import redis

# Connect to Memurai (Redis) running locally
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Redis keys
URL_QUEUE = 'crawler:queue'         # Redis List: distributed task queue
VISITED_SET = 'crawler:visited'     # Redis Set: track visited URLs
RESULTS_HASH = 'crawler:results'    # Redis Hash: store crawl results

# Endpoint to accept a list of URLs and enqueue them for crawling
@app.route('/submit-urls', methods=['POST'])
def submit_urls():
	data = request.get_json()
	urls = data.get('urls', [])
	added = 0
	for url in urls:
		# Add URL to queue only if not already visited
		if not r.sismember(VISITED_SET, url):
			r.rpush(URL_QUEUE, url)  # Push to task queue (Redis List)
			added += 1
	return jsonify({'status': 'ok', 'urls_added': added}), 200

# Endpoint to retrieve aggregated crawl results
@app.route('/results', methods=['GET'])
def get_results():
	# Get all results from Redis Hash
	raw_results = r.hgetall(RESULTS_HASH)
	results = {}
	for url, data in raw_results.items():
		# Try to decode data as JSON, even if double-encoded
		parsed = data
		try:
			if isinstance(parsed, str):
				parsed = json.loads(parsed)
			# If still a string, try one more time (for double-encoded cases)
			if isinstance(parsed, str):
				parsed = json.loads(parsed)
		except Exception:
			parsed = {'error': data}
		results[url] = parsed
	return jsonify({'results': results}), 200

# Endpoint to get current worker activity
@app.route('/workers', methods=['GET'])
def get_workers():
	workers = r.hgetall('crawler:workers')
	return jsonify({'workers': workers}), 200


# (Optional) Endpoint to check crawl progress
@app.route('/progress', methods=['GET'])
def get_progress():
	total_queued = r.llen(URL_QUEUE)
	total_visited = r.scard(VISITED_SET)
	total_results = r.hlen(RESULTS_HASH)
	# Count failed results (those with an 'error' field)
	failed = 0
	all_results = r.hgetall(RESULTS_HASH)
	import json
	for v in all_results.values():
		try:
			data = v
			if isinstance(data, str):
				data = json.loads(data)
			if 'error' in data:
				failed += 1
		except Exception:
			continue
	return jsonify({
		'queued': total_queued,
		'visited': total_visited,
		'results': total_results,
		'failed': failed
	}), 200

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True)
