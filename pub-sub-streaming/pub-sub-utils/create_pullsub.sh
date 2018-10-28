#gcloud beta pubsub subscriptions create YOUR_SUBSCRIPTION_NAME \
#    --topic YOUR_TOPIC_NAME \
#    --push-endpoint \
#    https://YOUR_PROJECT_ID.appspot.com/pubsub/push?token=YOUR_TOKEN \
#    --ack-deadline 10
YOUR_TOKEN=1223334444
PROJECT="wild-yukikaze"
gcloud beta pubsub subscriptions create testSub1 \
    --topic testTopic1 \
    --push-endpoint \
    https://${PROJECT}.appspot.com/pubsub/push?token=${YOUR_TOKEN} \
    --ack-deadline 10
