import os
import json
from datetime import datetime

from google.cloud import pubsub_v1
from google.cloud import firestore
from flask import Flask, request

# Environment variables
PROJECT_ID = os.environ["PROJECT_ID"]
TOPIC_ID = os.environ["TOPIC_ID"]
COLLECTION_NAME = os.environ["COLLECTION_NAME"]

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Initialize Firestore client
db = firestore.Client()

app = Flask(__name__)

@app.route('/', methods=['POST'])
def cloud_run_handler():
    """
    Handles incoming Cloud Tasks requests to the Cloud Run service.

    Returns:
        A dictionary indicating success or failure, with a 200 OK response.
    """
    try:
        # Access the message data from the request body
        print('request')
        print(request)
        data = request.get_json()
        print('data')
        print(data)
        messages = data.get('messages', [])  # Assuming 'messages' is the key

        for message in messages:
            message_data = {
                'message-id': message['messageId'],
                'body': message['body'],
                'timestamp': datetime.now().isoformat()
            }

            # Publish message to Pub/Sub
            future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
            print(f"Message sent to Pub/Sub: {message_data}")

            # Write to Firestore
            doc_ref = db.collection(COLLECTION_NAME).document()  # Auto-generate document ID
            doc_ref.set(message_data)
            print(f"Message sent to Firestore: {message_data}")

        return {'success': True}, 200

    except Exception as e:
        print(e)
        return {'success': False, 'error': str(e)}, 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
