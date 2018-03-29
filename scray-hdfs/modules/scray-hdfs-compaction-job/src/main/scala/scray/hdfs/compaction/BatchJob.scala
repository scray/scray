package scray.hdfs.compaction

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.hdfs.index.format.sequence.types.IndexValue

import scray.querying.sync.JobInfo
import org.apache.hadoop.io.SequenceFile
import scray.hdfs.compaction.conf.CompactionJobParameter
import scray.hdfs.index.format.sequence.types.Blob
import org.apache.spark.rdd.RDD
import scray.hdfs.index.format.sequence.SequenceFileWriter
import java.util.UUID.randomUUID



/**
 * Class containing all the batch stuff
 */
class BatchJob(@transient val sc: SparkContext, jobInfo: JobInfo[Statement, Insert, ResultSet], params: CompactionJobParameter) extends LazyLogging with Serializable {

  def batchAggregate() = {
    
    import org.apache.spark._
    val newestData = sc.sequenceFile(params.dataFilesInputPath + "/*.blob",  classOf[Text], classOf[Blob]).
      map(line => (line._1.toString(), new Blob(line._2.getUpdateTime, line._2.getData))).
      reduceByKey(returnNewestValue(_, _))
      
      writeToHDFS(newestData)
  }
  
  def returnNewestValue(indexValueA: Blob, indexValueB: Blob): Blob = {
      if(indexValueA.getUpdateTime > indexValueB.getUpdateTime) {
        indexValueA
      } else {
        indexValueB
      }
  }
  
  def writeToHDFS(compactedData: RDD[Tuple2[String, Blob]]) = {
    
    @transient lazy val hdfsWriter =  new SequenceFileWriter(s"${params.dataFilesOutputPath}/scray-data-${randomUUID().toString}-${System.currentTimeMillis()}")
    
    compactedData.foreachPartition { partitions =>
      partitions.foreach { newestElement =>
        hdfsWriter.insert(newestElement)
      }
    }
  }

}