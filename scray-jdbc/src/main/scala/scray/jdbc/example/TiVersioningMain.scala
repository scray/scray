package scray.jdbc.example

import scray.jdbc.extractors.ScraySQLDialect
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.jdbc.sync.JDBCDbSession
import scray.querying.sync.JobInfo
import scray.jdbc.sync.JDBCJobInfo

object TiVersioningMain {
  
  def main(args: Array[String]) {

    val connection = new JDBCDbSession("jdbc:mariadb://localhost:3306/scray", "scray", "scray")      
    val syncApi = new OnlineBatchSyncJDBC(connection)  
    val jobInfo = new JDBCJobInfo("job1")
    
    //syncApi.initJob(jobInfo)
    
    syncApi.startNextBatchJob(jobInfo)
  
    syncApi.completeBatchJob(jobInfo)
    
    println(syncApi.getQueryableBatchData(jobInfo))

  }
}