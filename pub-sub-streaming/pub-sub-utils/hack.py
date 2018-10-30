import os
import time
for i in range(100000):
  os.system(f'gcloud pubsub topics publish testTopic2 --message "hello {i:04d}"')
