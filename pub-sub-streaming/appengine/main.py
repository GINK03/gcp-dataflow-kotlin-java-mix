from flask import Flask, request, jsonify
import json
from google.cloud import pubsub_v1
import google.auth
from google.oauth2 import service_account

info = json.load(open('./pubsub-publisher.json'))
credentials = service_account.Credentials.from_service_account_info(info)

app = Flask(__name__)

@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return 'Hello World!'

@app.route("/json",  methods=['GET', 'POST'])
def json_path():
	content = request.json	
	print(content)

	publisher = pubsub_v1.PublisherClient(credentials=credentials)

	project_id = 'dena-ai-training-16-gcp'
	topic_name = 'testTopic3'
	topic_path = publisher.topic_path(project_id, topic_name)

	data = json.dumps(content)	
	future = publisher.publish(topic_path, data=data.encode('utf8'))
	return f"<h1 style='color:blue'>{content['username']} {future.result()}</h1>"

if __name__ == '__main__':
	app.run(host='127.0.0.1', port=8080, debug=True)
