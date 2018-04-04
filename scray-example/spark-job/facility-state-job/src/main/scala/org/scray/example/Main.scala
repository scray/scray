package org.scray.example

import scray.jdbc.sync.JDBCJobInfo
import org.scray.example.conf.JobParameter
import org.scray.example.input.KafkaOffsetReader

object Main {
  def main(args: Array[String]): Unit = {
    val jobInfo = JDBCJobInfo("test", 3, 2)
    val config = JobParameter()
    
    config.kafkaBootstrapServers = "kafka-broker-1:9092"
    
   val offsetReader = new KafkaOffsetReader(config.kafkaBootstrapServers)

    //FacilityStateJob.getAndPersistCurrentKafkaOffsets(jobInfo, config)
    
    offsetReader.getCurrentKafkaHighestOffsets("Facilities").map(x => println("High " + x))
    offsetReader.getCurrentKafkaLowestOffsets("Facilities").map(x => println("Low " + x))

  }
}