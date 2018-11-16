
import os

for i in range(100):
		os.system('curl -d \'{"key1":"value1", "username":"value {' + str(i) + '}"}\' -H "Content-Type: application/json" -X POST https://dena-ai-training-16-gcp.appspot.com/json')
