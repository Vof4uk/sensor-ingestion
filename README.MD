## In our monitoring system we have a number of device sensors which are sending time series data for many metrics in this format:

```json
{
    "environmentName": "",
    "deviceName": "",
    "metric": "",
    "value": xxxxxx.xx,
    "timestamp": epoch
}
```

for example:
```json
{
    "environmentName": "apartment 128",
    "deviceName": "studio hygrometer",
    "metric": "humidity",
    "value": 52,
    "timestamp": 1655366340
}
```


Devices may send the events only several times a day if status is slowly changed or they can send them regularly (every minute or hour) if there are continuous fast changes in measurements.
This series data is sent to our event driven stream processing pipeline.
Implement a {STREAMING_TECHNOLOGY} stateful pipeline which will stream data from a {MESSAGING_SYSTEM}, create state snapshot by user command received from {MESSAGING_SYSTEM} and send the resulting report to {MESSAGING_SYSTEM}


## Functional requirements:

1. Pipeline receives series data with measurements by metrics from devices using Kafka
2. User of the system shall be able to interact with the pipeline by sending commands to Kafka. User shall be able to send a command "report" to get the latest status (snapshot) for all devices, metrics on a particular environment.
3. Right after receiving the "report" command the system will send back to user the latest measurements to the {MESSAGING_SYSTEM} so the user will be able to handle them and show them on the GUI dashboard.


## Non functional requirements:

1. The system shall respond on commands during 1 minute and the status sent shall be actual for this time;
2. The pipeline shall write checkpoints to be able to recover state after distatster;
3. The dashboarding is out of scope for this solution;
4. Saving all the time series in database is not required on this stage;

## Proposed Solution:
FR1: The Spark Streaming is connected to Kafka messaging cluster. All sensor reading are sent through Kafka and delivered according to the Kafka setup.
The Spark cluster consumes messages via the flow described in [SensorDataFeed.scala](./src/main/scala/com/vmykytenko/sensors/collect/SensorDataFeed.scala).
The user will send Kafka message key along with the Kafka message value. The key will contain `environmentName`, so It will be possible shard and scale the 
data feed system for massive `environmentName`s.
The flow is quite simple as examine Kafka message and put to a storage in a partitioned manner. I chose Parquet over HDFS. Parquet is a columnar format
of storing data. It is well partitioned and effective for querying. HDFS as a file system for parquet will manage load, as the read requests number will grow.
FR2 and FR3: To obtain the report, use will have to send `SensorReportRequestKey` and `SensorReportRequest` over Kafka queue, on which the
[SensorDashboardQuery](./src/main/scala/com/vmykytenko/sensors/query/SensorDashboardQuery.scala) is configured to listen. `SensorReportRequestKey` is introduced
for finer control over Kafka partitioning and Spark cluster scaling. The Query part of the system will send response messages with `SensorReport`to a predefined
Kafka topic, passed as a configuration. To map requests with responses, each a correlation id `cid` is used. Client must provide it with every query message.
`SensorDashboardQuery` has a datasource of partitioned records in Parquet format. On each request it will filter out all with `environmentName` equal to same
from the request, leaving the latest from each unique combination of `deviceName` and `metric`. To achieve this I used Stream join Dataset on column `environmentName` and
developed a custom [Aggregation function](./src/main/scala/com/vmykytenko/sensors/query/ReportAggregator.scala). The Aggregated Report is serialized and sent
to the customer via configured Kafka topic in a format of `SensorReport`.

NFR-1: The system is not designed as fully consistent, it means that a user that sends a message with the data will see the changes in report state eventually.
With sufficient resources allocated to HDFS and Spark cluster it is possible to have the feedback time < 1 minute.
NFR-2: Checkpoints are implemented on top of Checkpoints mechanism of Spark and shall be persisted in HDFS.
NFR-3: - 
NFR-4: Although it was not required, all the events are saved to HDFS, it is part of design. It is relatively easy to set up a CRON and cleanup stale records, because of partitioning.

## Testing
I chose Testcontainers to emulate e2e tests. 

## Known issues:
1. The number of events will grow, and Join operation may become a bottleneck and expensive. Data cleanup should help.
2. As tests showed the latest micro-batches from the sensor feed are getting slowly to the Parquet storage, only if the feed keeps coming.
   