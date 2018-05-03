package scray.jdbc.example

import scray.jdbc.extractors.ScraySQLDialect
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.jdbc.sync.JDBCDbSession
import scray.querying.sync.JobInfo
import scray.jdbc.sync.JDBCJobInfo
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object TiVersioningMain {
  
  def main(args: Array[String]) {
    val rrrr = JDBCDbSession.getNewJDBCDbSession("jdbc:mariadb://127.0.0.1:3306/scray", "scray", "scray")
    .map (new OnlineBatchSyncJDBC(_))
    .map  { syncApi => 
 
    val jobInfo = new JDBCJobInfo("job1")
        
    //syncApi.initJob(jobInfo)
    
    val dateFormater = new SimpleDateFormat("yyyy-MM-dd")

    
    // Get latest completed download ore use first available data from 2016-06-20
    val lastBatchEndTime = syncApi.getLatestBatchMetadata(jobInfo).flatMap { _.batchEndTime }.getOrElse {
      val startTime = Calendar.getInstance()
      startTime.setTime(dateFormater.parse("2016-06-20"))
      startTime.getTimeInMillis
    }
   
    val startTime = Calendar.getInstance()
    startTime.setTimeInMillis(lastBatchEndTime)
    
    val endTime = Calendar.getInstance()
    endTime.set(Calendar.HOUR_OF_DAY, 0);
    endTime.set(Calendar.MINUTE, 0);
    endTime.set(Calendar.SECOND, 0);
    endTime.set(Calendar.MILLISECOND, 0);
    endTime.add(Calendar.DATE, -1)
   
    
    
    syncApi.startNextBatchJob(jobInfo)

    
    while(startTime.getTimeInMillis < endTime.getTimeInMillis) {
       //println( +"\t"+ dateFormater.format(endTime.getTime))
       
       val downloadURL = "http://history.adsbexchange.com/Aircraftlist.json/" + dateFormater.format(startTime.getTime) + ".zip"
       println("Affe \t" + downloadURL)
      startTime.add(Calendar.DATE, 1)
    }
    
      
    syncApi.completeBatchJob(jobInfo, endTime.getTimeInMillis)
//    
//    println(syncApi.getQueryableBatchData(jobInfo))
    } // recover {
//      case e: Throwable => println(e)
//    }
  }
}