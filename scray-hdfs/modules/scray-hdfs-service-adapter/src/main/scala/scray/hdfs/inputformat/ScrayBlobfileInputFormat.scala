package scray.hdfs.inputformat

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import java.util.{List => JList} 
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.TaskAttemptContext
import java.util.ArrayList

class ScraySingleBlobfileInputFormat[T <: Writable] extends InputFormat[T, BytesWritable] {
  import ScraySingleBlobfileInputFormat._
  
  // TODO: TBD
  override def getSplits(job: JobContext): JList[InputSplit] = {
    val splits = Option(job.getConfiguration.get(SCRAY_BLOB_INPUTFORMAT_NUMBER_OF_SPLITS)).
      map(_.toInt).getOrElse(DEFAULT_MAGIC_NUMBER_OF_SPLITS)
    new ArrayList[InputSplit]()
  }

  // TODO: TBD
  override def createRecordReader(split: InputSplit, tac: TaskAttemptContext): RecordReader[T, BytesWritable] = {
    null
  }
  
}

object ScraySingleBlobfileInputFormat {
  
  val DEFAULT_MAGIC_NUMBER_OF_SPLITS = 10
  val SCRAY_BLOB_INPUTFORMAT_NUMBER_OF_SPLITS = "scray.blob.splits.number"
  
}