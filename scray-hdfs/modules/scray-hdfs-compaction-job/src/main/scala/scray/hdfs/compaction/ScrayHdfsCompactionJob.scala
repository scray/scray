package scray.hdfs.compaction

import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.hdfs.compaction.cli.{Config, Options}
import scala.collection.mutable.Queue
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.cassandra.sync.CassandraImplementation._
import scray.querying.sync.OnlineBatchSync
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.JobInfo
import scray.querying.sync.AbstractRow
import scray.cassandra.sync.CassandraJobInfo
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import scray.hdfs.compaction.conf.CompactionJobParameter

/**
 * @author <author@name.org>
 */
object ScrayHdfsCompactionJob extends LazyLogging {
  
  /**
   * creates a Spark Streaming context
   */
  def setupSparkStreamingConfig(masterURL: String, seconds: Int, config: Config, jobInfo: CassandraJobInfo): () => StreamingContext = () => { 
    logger.info(s"Connecting streaming context to Spark on $masterURL and micro batches with $seconds s")
    val conf = new SparkConf().setAppName("Stream: " + this.getClass.getName).setMaster(masterURL)
    val ssc = new StreamingContext(conf, Seconds(seconds))
    
    ssc.checkpoint(config.checkpointPath)
    val streamingJob = new StreamingJob(ssc, jobInfo)
    val dstream = streamSetup(ssc, streamingJob, config)
    
    ssc
  }
  
  def setupSparkBatchConfig(masterURL: String): () => SparkContext = () => {
    logger.info(s"Connecting batch context to Spark on $masterURL")
    new SparkContext(new SparkConf().setAppName("Batch: " + this.getClass.getName).setMaster(masterURL))
  }

  val queue = new Queue[RDD[String]]()

  def streamSetup(ssc: StreamingContext, streamingJob: StreamingJob, config: Config): Unit = {
    // example how to stream from Kafka
    val dstream = config.kafkaDStreamURL.flatMap { url =>
      {
        StreamingDStreams.getKafkaStringSource(
          ssc,
          config.kafkaDStreamURL,
          config.kafkaTopic.getOrElse({
            logger.warn("Kakfa topic not defined. Use scray")
            Array("scray")
          }))
      }
    }.getOrElse(
        {
          logger.error(s"Unable to create kafka stream. Use predefined example data from ${this.getClass.getName}")
          ssc.queueStream(queue)
        }
    )
    
    dstream.checkpoint(Duration(config.checkpointDuration))
    streamingJob.runTuple(dstream, config.cassandraHost, config.cassandraKeyspace, ssc.sparkContext.master)    
  }

  /**
   * TODO: remove, since this is only used for the initial setup
   */
  def streamSomeBatches(ssc: StreamingContext) = {
    val data = Array("test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8")
    (0 until 40).foreach { _ =>
      queue += ssc.sparkContext.parallelize(data)
    }
  }

  /**
   * execute batch function
   */
  def batch(config: Config) = {
    logger.info(s"Using Batch mode.")
    val jobInfo = new CassandraJobInfo("scray-hdfs-compaction-job", config.numberOfBatchVersions, config.numberOfOnlineVersions)
      val sc = setupSparkBatchConfig(config.master)()
      val batchJob = new BatchJob(sc, jobInfo, CompactionJobParameter())
      batchJob.batchAggregate()

  }

  /**
   * execute streaming function
   */
  def stream(config: Config) = {
    logger.info(s"Using HDFS-URL=${config.hdfsDStreamURL} and Kafka-URL=${config.kafkaDStreamURL}")
    val syncTable = new OnlineBatchSyncCassandra(config.cassandraHost.getOrElse("127.0.0.1"))
    val jobInfo = new CassandraJobInfo("scray-hdfs-compaction-job", config.numberOfBatchVersions, config.numberOfOnlineVersions)
    if(syncTable.startNextOnlineJob(jobInfo).isSuccess) {
      val ssc = StreamingContext.getOrCreate(config.checkpointPath, setupSparkStreamingConfig(config.master, config.seconds, config, jobInfo))

      // prepare to checkpoint in order to use some state (updateStateByKey)
      ssc.start()
      // TODO: write out zk information for closing this app  
      // log out random port to connect to
      // TODO: remove code to insert data
      streamSomeBatches(ssc)
  
      ssc.awaitTermination()
    } else {
      logger.error("Streaming table locked")
    }
  }
  
  object ExampleTable extends ArbitrarylyTypedRows {
    val time = new Column[Long]("time")
    val name = new Column[String]("name")
    val sum = new Column[Int]("sum")
         
    override val columns = time :: name :: sum :: Nil
    override val primaryKey = s"(${time.name})"
    override val indexes = None
  }

  def main(args : Array[String]): Unit = {
    Options.parse(args) match {
      case Some(config) =>

        config.batch match {
          case true =>  batch(config)
          case false => stream(config)
        }
      case None => {
        println("Error while parsing command line parameters")
        Options.parse(args)
      }
    }
  }

}
