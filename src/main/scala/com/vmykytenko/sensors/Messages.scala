package com.vmykytenko.sensors

import java.sql.Timestamp

//{
//    "environmentName": "",
//    "deviceName": "",
//    "metric": "",
//    "value": xxxxxx.xx,
//    "timestamp": epoch
//}
case class SensorMessage(environmentName: String,
                         deviceName: String,
                         metric: String,
                         value: Double,
                         timestamp: Long)

case class SensorMessageKey(environmentName: String,
                            deviceName: String)

case class SensorReportRequestKey(environmentName: String)

case class SensorReportRequest(environmentName: String, cid: String)

case class SensorReportItem(environmentName: String,
                            deviceName: String,
                            metric: String,
                            value: Double,
                            timestamp: Long)

case class SensorReport(cid: String, items: Array[SensorReportItem])

case class SensorReportKey(environmentName: String)