## pyflink
we are installing python3.7 separately with Dockerfile.flink to make it compatible with the flink image
run docker compose up to start the container (run all code within the 06 folder)
view pyflink on 8081
I am using pgweb for viewing the db
```bash
# Now continue with the installation
wget -O pgweb.zip https://github.com/sosedoff/pgweb/releases/download/v0.16.2/pgweb_linux_amd64.zip
unzip pgweb.zip
chmod +x pgweb_linux_amd64
sudo mv pgweb_linux_amd64 /usr/local/bin/pgweb
```
port should be 8080 but it is already in use, so we use 8085
```bash
pgweb --host=localhost --user=postgres --pass=postgres --db=postgres --bind=0.0.0.0 --listen=8085
```
create a processed_event table schema 

```sql
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

SELECT COUNT(*) FROM processed_events;
```


view pyflink jobmanager on 8081
## create producer.py kafka file and run with python (the data should be accepted by redpanda) and start_job.py
1️⃣ The producer (producer.py) sends messages to Redpanda on the Kafka topic (test-topic).
2️⃣ Redpanda stores and manages the messages, acting as a Kafka broker.
3️⃣ Flink (start_job.py) subscribes to Redpanda's Kafka topic, extracts timestamps, and assigns watermarks, and streams data into PostgreSQL (processed_events).

for the code to run, I had to create a new env with python version 3.7.9 on my host machine or I move all my code to the Docker container
```bash
conda create --name flink python=3.7.9
conda activate flink
sudo apt-get update -y
sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev wget
pip install --upgrade pip
pip install -r requirements.txt --no-cache-dir
```

The new env is unnecessary since the code will run within docker. 
so, we can run 
```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job3.py --pyFiles /opt/src -d
```


debug if redpanda receives data
```bash
docker exec -it redpanda-1 rpk topic consume test-topic --brokers=redpanda-1:29092
```

access flink-jobmanager using
```bash
docker exec -it flink-jobmanager bash
```

I was not getting any data because redpanda already had data before I ran pyflink and I was using latest data, so I change to earliest to ensure 
that it started reading from the beginning. Here is some note on that

Flink's Kafka Connector and scan.startup.mode
Flink allows you to configure how it handles data in the source (in this case, Kafka) when the program restarts. The key setting is scan.startup.mode, and it controls where Flink begins reading from the Kafka topic after the job is restarted:

latest-offset:
This option means that Flink will start reading from the latest offset (the most recent message) in the Kafka topic when the job is restarted. Any messages that were published to Kafka before the job started again will be skipped.
Use case: If you only care about processing new messages arriving after the restart and don't want to reprocess old messages.

earliest-offset:
This option means that Flink will start reading from the earliest offset (the oldest message) in the Kafka topic. If the job is restarted, Flink will reprocess messages from the beginning of the Kafka topic.
Use case: If you want to ensure that no messages are skipped, and you want to reprocess all the data from the start (even messages that arrived before the job was restarted).

group-offsets:
This option tells Flink to resume from where it last left off (i.e., it uses the offset stored in Kafka consumer group metadata).
Use case: If you want Flink to pick up where it left off (this is often used with persistent state to ensure that data processing continues seamlessly).


now, we can create a processed_events_aggregated schema
```sql
CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits BIGINT
);

SELECT COUNT(*) FROM processed_events_aggregated;
```

Allowed lateness and side_output_late_data in flink