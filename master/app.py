
import uuid
import time

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
	# Step 1: Generate a unique session ID
	session_id = str(uuid.uuid4())
	start_time = int(time.time())
	# Step 2: Store session info in Redis
	session_key = f'crawler:session:{session_id}'
	r.hmset(session_key, {
		'session_id': session_id,
		'urls': json.dumps(urls),
		'start_time': start_time,
		'status': 'active',
		'submitted_count': len(urls)
	})
	r.lpush('crawler:sessions', session_id)  # Add to session history list
	# Step 3: Push URLs to queue as before
	added = 0
	already_crawled = []
	for url in urls:
		print(f"[DEBUG] Submitting url: {url} (type: {type(url)})")
		if not r.sismember(VISITED_SET, url):
			task = json.dumps({'session_id': session_id, 'url': url})
			print(f"[DEBUG] Enqueuing task: {task}")
			r.rpush(URL_QUEUE, task)
			added += 1
		else:
			already_crawled.append(url)
	return jsonify({'status': 'ok', 'urls_added': added, 'already_crawled': already_crawled, 'session_id': session_id}), 200


# Endpoint to retrieve aggregated crawl results (all sessions, legacy)
@app.route('/results', methods=['GET'])
def get_results():
	raw_results = r.hgetall(RESULTS_HASH)
	results = {}
	for url, data in raw_results.items():
		parsed = data
		try:
			if isinstance(parsed, str):
				parsed = json.loads(parsed)
			if isinstance(parsed, str):
				parsed = json.loads(parsed)
		except Exception:
			parsed = {'error': data}
		results[url] = parsed
	return jsonify({'results': results}), 200

# Endpoint to retrieve crawl results for a specific session
@app.route('/results/<session_id>', methods=['GET'])
def get_session_results(session_id):
	session_results_hash = f"crawler:results:{session_id}"
	raw_results = r.hgetall(session_results_hash)
	results = {}
	for url, data in raw_results.items():
		parsed = data
		try:
			if isinstance(parsed, str):
				parsed = json.loads(parsed)
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

# Endpoint to list all crawl sessions with summary stats
@app.route('/sessions', methods=['GET'])
def list_sessions():
	session_ids = r.lrange('crawler:sessions', 0, -1)
	sessions = []
	for session_id in session_ids:
		session_key = f'crawler:session:{session_id}'
		info = r.hgetall(session_key)
		if not info:
			continue
		try:
			urls = json.loads(info.get('urls', '[]'))
		except Exception:
			urls = []
		submitted = int(info.get('submitted_count', len(urls)))
		start_time = int(info.get('start_time', 0))
		# Get results for this session
		results_hash = f'crawler:results:{session_id}'
		results = r.hgetall(results_hash)
		completed = len(results)
		failed = 0
		for v in results.values():
			try:
				data = v
				if isinstance(data, str):
					data = json.loads(data)
				if 'error' in data:
					failed += 1
			except Exception:
				continue
		sessions.append({
			'session_id': session_id,
			'submitted': submitted,
			'completed': completed,
			'failed': failed,
			'start_time': start_time
		})
	return jsonify({'sessions': sessions}), 200

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True)
