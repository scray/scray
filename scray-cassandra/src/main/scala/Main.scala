

import scray.cassandra.sync.CassandraJobInfo
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.ColumnWithValue
import com.datastax.driver.core.Cluster
import scray.cassandra.sync.CassandraDbSession
import com.datastax.driver.core.querybuilder.QueryBuilder

object Main {
  def main(args: Array[String]) {

    val table = new OnlineBatchSyncCassandra("10.11.22.31")
    val jobInfo = new CassandraJobInfo("abc", 5)
        
    
    val abc = BatchOutputTable.table.columns
    
    table.initJob(jobInfo, BatchOutputTable.table.columns)

    
    for (a <- 1 until 500000) {
      
      val prevSlot = table.getLatestBatch(jobInfo).getOrElse(0)
      val lastBatchData = table.getBatchJobData(jobInfo.name, prevSlot, BatchOutputTable.row)

      println("PrevSlot \t" + prevSlot)

      
      table.startNextBatchJob(jobInfo)
      val slot = table.getLatestBatch(jobInfo).getOrElse(0)

      println("Start next batch Job: " + slot)
      
      println("Write batch data")
      val newCount: Int = lastBatchData.map { x => x.head.columns.tail.head.value.asInstanceOf[Int]}.getOrElse(1)
      
      table.insertInBatchTable(jobInfo, slot, BatchOutputTable.setCounter(newCount))
      println("New counter" +  newCount)
      
      Thread.sleep(10000)
   
      println("Complete batch job: " + slot)
      table.completeBatchJob(jobInfo)
      
     
      
    }

    

    //table.insertInBatchTable(jobInfo, table.getLatestBatch(jobInfo).getOrElse(0), setValue(0))
    //
    //    table.completeBatchJob(jobInfo)
    //
    //    while (true) {
    //            
    //      val latestData = table.getBatchJobData(jobInfo.name, table.getLatestBatch(jobInfo).getOrElse(0), BatchOutputTable.row)
    //
    //      //val newValue = latestData.get.head.columns.tail.head.value.asInstanceOf[Int] + 1
    //      table.startNextBatchJob(jobInfo)
    //
    //      //table.insertInBatchTable(jobInfo, table.getLatestBatch(jobInfo).getOrElse(0), setValue(newValue))
    //      
    //      table.completeBatchJob(jobInfo)
    //      Thread.sleep(5000)
    //    }
    //
    //
    //      println(BatchOutputTable.row.columns.head)
    //      println(BatchOutputTable.row.columns.tail.head)
    //
    //      BatchOutputTable.row
    //    }
    //
    //    //    table.completeBatchJob(jobInfo)
  }
}