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

* Pipeline receives series data with measurements by metrics from devices using {MESSAGING_SYSTEM}
* User of the system shall be able to interact with the pipeline by sending commands to {MESSAGING_SYSTEM}. User shall be able to send a command "report" to get the latest status (snapshot) for all devices, metrics on a particular environment.
* Right after receiving the "report" command the system will send back to user the latest measurements to the {MESSAGING_SYSTEM} so the user will be able to handle them and show them on the GUI dashboard.


## Non functional requirements:

* The system shall respond on commands during 1 minute and the status sent shall be actual for this time;
* The pipeline shall write checkpoints to be able to recover state after distatster;
* The dashboarding is out of scope for this solution;
* Saving all the time series in database is not required on this stage;