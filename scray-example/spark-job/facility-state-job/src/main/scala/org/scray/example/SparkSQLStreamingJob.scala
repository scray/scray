package org.scray.example

import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scray.example.conf.JobParameter
import org.scray.example.data.JsonFacilityParser
import org.scray.example.output.GraphiteForeachWriter
import org.scray.example.data.Facility
import org.scray.example.data.FacilityStateCounter
import java.sql.Timestamp

class SparkSQLStreamingJob(spark: SparkSession, conf: JobParameter) extends Serializable {

  def run(startTime: Long) = {
    import spark.implicits._

    val calStartTime = Calendar.getInstance()
    calStartTime.setTimeInMillis(startTime)
    
    @transient lazy val graphiteWriter = new GraphiteForeachWriter(conf.graphiteHost, conf.graphitePort, conf.graphiteRetries)
    @transient lazy val faciliyJsonParser = new JsonFacilityParser
    
    // Connect to kafa stream
    val kafkaSource = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", conf.kafkaBootstrapServers)
      .option("subscribe", conf.kafkaTopic)
      .load()

    // Read and parse data from kafka stream
    val facilityElement = kafkaSource
      .select($"value" cast "string")  // Take column 'value' from kafka stream as string
      .as[String]
      .flatMap((faciliyJsonParser.parse))  // Parse given string
      
      // Prepare data
      // Spark SQL requires time stamp as java.sql.Timestamp
      val preparedFacilityElement = facilityElement.map(facility => 
        Facility(facility.facilitytype, facility.state, new Timestamp(facility.timestamp)))
        
      
    // Aggregate data
        
    // Group the facilities by type and state and compute the count of each group    
    // For details https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
    val aggregatedFacilityData = preparedFacilityElement.
      withWatermark("timestamp", conf.watermark).
      groupBy(
        window(column("timestamp"), conf.windowDuration, conf.slideDuration, calStartTime.get(Calendar.SECOND) + " seconds"),
        column("facilityType"),
        column("state")
      ).count()
      .select("facilityType", "state", "count")

    // Write data to graphite   
    aggregatedFacilityData.as[FacilityStateCounter]
      .writeStream.foreach { graphiteWriter }
      .start()
      .awaitTermination()

  }
}