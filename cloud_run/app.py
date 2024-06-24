import os
from flask import Flask, request, render_template
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable

app = Flask(__name__)

project_id=os.environ.get('PROJECT_ID')
topic_name=os.environ.get('TOPIC_NAME')
# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_name)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/publish", methods=["POST"])
def publish_message():
    if request.method == "POST":
        message = request.form["message"]
        publish_message_to_pubsub(message)
        return "Message sent to Pub/Sub topic!"

def publish_message_to_pubsub(message):
    publish_futures=[]    
    # When you publish a message, the client returns a future.
    publish_future = publisher.publish(topic_path, message.encode("utf-8"))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, message))
    publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Published message: {message}")

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
