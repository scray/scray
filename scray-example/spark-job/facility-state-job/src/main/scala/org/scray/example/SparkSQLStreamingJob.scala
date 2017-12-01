package org.scray.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.TimestampType
import java.sql.Timestamp
import scray.example.input.db.fasta.model.Facility
import org.scray.example.output.GraphiteForeachWriter
import org.apache.spark.sql.types.DataType
import org.scray.example.conf.JobParameter
import java.util.Calendar

case class FacilityStateCounter(facilityType: String, state: String, count: Long)

class SparkSQLStreamingJob(spark: SparkSession, conf: JobParameter) {

  def run(startTime: Long) = {
    import spark.implicits._

    val calStartTime = Calendar.getInstance()
    calStartTime.setTimeInMillis(startTime)
    
    println("ddddddddddddddddddddd")
    
    val graphiteWriter = new GraphiteForeachWriter(conf.graphiteHost, conf.graphitePort, conf.graphiteRetries)

    // Connect to kafa stream
    val kafkaSource = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", conf.kafkaBootstrapServers)
      .option("subscribe", conf.kafkaTopic)
      .load()

    val schema = DataType.fromJson(conf.kafkaDataSchemaAsJsonExample).asInstanceOf[StructType]

    // Parse kafka json data
    val facilityElement = kafkaSource
      .select($"value" cast "string" as "json")
      .select(from_json($"json", schema) as "data")
      .select("data.type", "data.state", "data.timestamp")
      .select(column("type").alias("facilityType"), $"state", to_timestamp(from_unixtime($"timestamp" / 1000L)) as "timestamp")

      
      
    // Aggregate data
    val aggregatedFacilityData = facilityElement.
      withWatermark("timestamp", conf.watermark).
      groupBy(
        window(column("timestamp"), conf.windowDuration, conf.slideDuration, calStartTime.get(Calendar.SECOND) + " seconds"),
        column("facilityType"),
        column("state")).count().
        select("facilityType", "state", "count")

    // Write data to graphite   
    aggregatedFacilityData.as[FacilityStateCounter]
      .writeStream.foreach { graphiteWriter }
      .start()
      .awaitTermination()

  }
}