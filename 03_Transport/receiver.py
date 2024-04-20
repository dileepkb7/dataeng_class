from google.cloud import pubsub_v1
#g
project_id = "dataeng-activity"
subscription_id = "MySub"

def receive_messages(project_id, subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        print(f"Received message: {message.data.decode('utf-8')}")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    print(f"Listening for messages on {subscription_path}..")
    # Keep the receiver script running to continuously consume messages
    try:
        streaming_pull_future.result()  # Blocking call
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    receive_messages(project_id, subscription_id)