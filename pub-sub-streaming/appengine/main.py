from flask import Flask, request, jsonify
import json
app = Flask(__name__)

@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return 'Hello World!'

@app.route("/json",  methods=['GET', 'POST'])
def json():
	content = request.json	
	print(content)
	return f"<h1 style='color:blue'>{content['username']}</h1>"

if __name__ == '__main__':
	app.run(host='127.0.0.1', port=8080, debug=True)
