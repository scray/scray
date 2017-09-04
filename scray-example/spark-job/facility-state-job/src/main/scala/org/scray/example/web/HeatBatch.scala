package de.s_node.analyser.web

import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.cassandra.sync.CassandraJobInfo
import scray.querying.sync.RowWithValue
import de.s_node.analyser.data.DayAvailabilityTable

object HeatBatch {

  def main(args: Array[String]) {
    val table = new OnlineBatchSyncCassandra("2a02:8071:3197:2b00:f64d:30ff:fe66:a343")
    val jobInfo = new CassandraJobInfo("DayAvailability", 5)

    // Prepare database 
    table.initJob(jobInfo, DayAvailabilityTable.table.columns)

    val lastBatchData: List[RowWithValue] = table.getBatchJobData(jobInfo, DayAvailabilityTable.row).getOrElse({
      println(".................................................")
      table.startNextBatchJob(jobInfo)
      table.insertInBatchTable(jobInfo, DayAvailabilityTable.setAvailability("4711", System.currentTimeMillis(), (0)))
      table.completeBatchJob(jobInfo)
      println(".................................................")
      table.getBatchJobData(jobInfo, DayAvailabilityTable.row).get
    })

    table.startNextBatchJob(jobInfo)

    lastBatchData.map { row =>
      {
        val availCol = row.getColumn(DayAvailabilityTable.availability).get
        val timeCol = row.getColumn(DayAvailabilityTable.time).get
        println("////////////////////////////////////////////")
        
        if(timeCol.value == 1829) {
          table.insertInBatchTable(jobInfo, DayAvailabilityTable.setAvailability("4711", 1829, (availCol.value + 100)))
        }
        println("-------------------------------")
      }
    }
    table.completeBatchJob(jobInfo)
  }
}