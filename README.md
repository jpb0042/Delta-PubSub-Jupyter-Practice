# Delta-PubSub-Jupyter-Practice
**This repo creates a delta table in a Jupyter notebook, publishes the table to pub/sub, and reads/acknowledges the messages back from pub/sub.**

### Prerequisites
1.  **WSL and Docker**
Windows: [Install WSL](https://learn.microsoft.com/en-us/windows/wsl/install) (Windows Subsystem for Linux) to run docker. [Download docker desktop](https://www.docker.com/products/docker-desktop/). Follow the installation wizard and open the app.
Mac: [Install instructions](https://docs.docker.com/desktop/install/mac-install/)

2. **GCP (Google Cloud Platform)**
A GCP project and service account with a pub/sub topic is required for this notebook. The `.json` file associated with the service account is used to publish messages to pub/sub. 

### Running the container

1. Open a terminal (Powershell or equivalent) and clone the repo to a location of your choice.

2. `cd` into the `delta-pubsub-jupyter-practice` file. 

3. Open the project in VS Code using  `code .` or another editor of your choice.

4. ***Optional:*** edit the `ACCESS_TOKEN` in the `.env` file `delta-pubsub-jupyter-practice/.env`

5. In the terminal, use `docker compose -f jupyter-local-spark-compose.yml up` to start your container.  Use the `-d` flag to start your container in detached mode.

6. The container should now be running and viewable in your docker desktop app.

7. Open [localhost:8889](http://localhost:8889/) in your browser and enter the `ACCESS_TOKEN` stored in the `.env` file to log in. 

8. Open the `PracticeWriteRead.ipynb` file to run/edit the Jupyter notebook. The notebook needs to be run twice in to show that the messages have been recieved back from pub/sub.

9. Use `docker compose -f jupyter-local-spark-compose.yml down` to tear down your deployment when you are finished using it. 

### About the notebook
**Delta Lake:** Delta Lake is an open-source storage layer developed by Databricks that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to Apache Spark and big data workloads. It is designed to improve the reliability, performance, and scalability of big data processing and analytics. 

The following lines of code create a data frame that will be used in the delta table:
```python
# Create a dataframe
data = [(1, 'Alice', '555-555-5555'), (2, 'Bob', '123-456-7890'), (3, 'Charlie', '098-876-5432'), (4, 'Benny', '000-000-0000')]
columns = ['id', 'name', 'phone']
df = spark.createDataFrame(data, columns)
```
The following lines of code store the dataframe in a new delta table:
```python
# Desired location of delta table.
delta_log_path = "desired/path"
# Creates a folder in the specified location and writes the dataframe to a delta table within that folder
df.write.format("delta").mode("overwrite").save(delta_log_path)
```
The following lines of code read the delta table and display its contents to the console:
```python
# Load the Delta table
delta_table = DeltaTable.forPath(spark, "delta")

# Read the data from the Delta table
delta_data = delta_table.toDF()

# Show the data to ensure the table was created properly
delta_data.show()
```
**Pub/Sub:** Google Cloud Pub/Sub is a messaging service provided by Google Cloud Platform that enables the creation and management of real-time messaging between independent applications. It is a fully managed, scalable, and durable message queue service that allows you to send and receive messages between different components of a distributed system.

The following lines of code are used to publish a message to pub/sub. Each message is a row of the delta table:
```python
# Create a Pub/Sub client
publisher = pubsub_v1.PublisherClient()

# Create a Topic path
topic_path = publisher.topic_path(project_id, topic_name)

# Load data from your Delta table into a DataFrame
delta_log_path = "desired/path"  # Replace with the actual path to your Delta table

#Read delta table
delta_df = spark.read.format("delta").load(delta_log_path)

# Convert the DataFrame to a list of dictionaries (each row as a dictionary)
data_to_publish = delta_df.toJSON().collect()

# Publish each record to Pub/Sub
for record in data_to_publish:
    # Convert the JSON record to bytes
    message_data = json.dumps(record).encode("utf-8")

    # Publish the message
    publisher.publish(topic_path, data=message_data)

    print(f"Published message: {record}")
```
The following lines of code are used to read messages back from pub/sub. The messages are acknowledged to remove them from the Pub/Sub message console in GCP.
```python
# Set the path to your service account key JSON file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/.json" # Replace with the path to your .json

# Replace with your GCP project id and subscription name
project_id = "your-project-id"
subscription_name = "your-subscription-id"

# Create a Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Create a subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):
    print(f"Received message: {message.data}")
    message.ack()  # Acknowledge the message to remove it from the subscription

# Open the subscription to start receiving messages
subscriber.subscribe(subscription_path, callback=callback)
```
