from google.cloud import pubsub_v1

project_id = "dataeng-activity"
subscription_id = "MySub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received and discarded message: {message.data.decode('utf-8')}")
    message.ack()

print(f"Listening for messages on {subscription_path}...")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Subscription canceled by user.")
    except TimeoutError:
        streaming_pull_future.cancel()
        print("Timed out waiting for messages.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        streaming_pull_future.cancel()

print("Finished listening for messages.")