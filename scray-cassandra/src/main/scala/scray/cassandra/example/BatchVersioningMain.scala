package scray.cassandra.example

import scray.cassandra.sync.CassandraJobInfo
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.ColumnWithValue
import com.datastax.driver.core.Cluster
import scray.cassandra.sync.CassandraDbSession
import com.datastax.driver.core.querybuilder.QueryBuilder

/**
 * Write 100 batch versions in 5 slots. 
 * Use old data for new calculation. 
 * newDate =  oldDate + 1
 */
object BatchVersioningMain {

  def main(args: Array[String]) {
    
    if(args.length != 1) {
      println("Hostname for cassandra cluster as parameter requiered")
      System.exit(0);
    } 

    val table = new OnlineBatchSyncCassandra(args(0))
    val jobInfo = new CassandraJobInfo("ScrayExample", 5)

    // Prepare database 
    table.initJob(jobInfo, BatchOutputTable.table.columns)

    for (x <- 1 until 100) {

      table.startNextBatchJob(jobInfo)

      // Get old data
      val lastBatchData = table.getBatchJobData(jobInfo, BatchOutputTable.row)
      
      // Create new data (Increment counter)
      val newCount: Int = lastBatchData.map {_.head.getColumn(BatchOutputTable.count).map { _.value }}.flatten.getOrElse(0) + 1
      Thread.sleep(5000)

      // Write new data
      table.insertInBatchTable(jobInfo, BatchOutputTable.setCounter(newCount))

      // Complete job
      table.completeBatchJob(jobInfo)
      
      println(s"\n Writen batch data ${newCount} \n")
      
    }

  }
}