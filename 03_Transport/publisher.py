from google.cloud import pubsub_v1
import json

project_id = 'dataeng-activity'
topic_name = 'MyTopic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Function to publish messages from JSON file
def publish_messages_from_file(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
        for record in data:
            message_data = json.dumps(record).encode('utf-8')
            future = publisher.publish(topic_path, data=message_data)
            print(f"Published message: {future.result()}")

# Publish messages from each JSON file
publish_messages_from_file('3951_vehicle_data.json')
publish_messages_from_file('3235_vehicle_data.json')