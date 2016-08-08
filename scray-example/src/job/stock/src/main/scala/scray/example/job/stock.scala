package scray.example.job

import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.example.job.cli.{Config, Options}
import scala.collection.mutable.Queue
import com.datastax.driver.core.Session
import scray.querying.sync.DbSession
import scray.cassandra.sync.CassandraDbSession
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.cassandra.sync.CassandraJobInfo
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.cassandra.sync.CassandraImplementation.{strType,lngType}



/**
 * @author <author@name.org>
 */
object stock extends LazyLogging {
  
  /**
   * creates a Spark Streaming context
   */
  def setupSparkStreamingConfig(masterURL: String, seconds: Int): () => StreamingContext = () => { 
    logger.info(s"Connecting streaming context to Spark on $masterURL and micro batches with $seconds s")
    val conf = new SparkConf().setAppName("Stream: " + this.getClass.getName).setMaster(masterURL)
    new StreamingContext(conf, Seconds(seconds))
  }
  
  def setupSparkBatchConfig(masterURL: String): () => SparkContext = () => {
    logger.info(s"Connecting batch context to Spark on $masterURL")
    new SparkContext(new SparkConf().setAppName("Batch: " + this.getClass.getName).setMaster(masterURL))
  }

  val queue = new Queue[RDD[(String, String, String, String)]]()

  def streamSetup(ssc: StreamingContext, streamingJob: StreamingJob, config: Config): Unit = {
    /* example how to stream from Kafka
    val dstream = config.kafkaDStreamURL.flatMap { url =>.
        StreamingDStreams.getKafkaCasAxSource(ssc, config.kafkaDStreamURL, config.kafkaTopic).map(_.map(_._2)) }.getOrElse {
          // shall fail if no stream source was provided on the command line
          config.hdfsDStreamURL.flatMap { _ => StreamingDStreams.getTextStreamSource(ssc, config.hdfsDStreamURL) }.
          map{_.map{ casaxStr =>
            val bytes = casaxStr.getBytes("UTF-8")
            decoder.fromBytes(bytes)}}.get
    } */
    val dstream = ssc.queueStream(queue)
    dstream.checkpoint(Duration(config.checkpointDuration))
    streamingJob.runTuple(dstream.map(x => (StreamingJob.buildAggregationKey(x), 1L)), config.cassandraHost, config.cassandraKeyspace, ssc.sparkContext.master)
  }

  /**
   * TODO: remove, since this is only used for the initial setup
   */
  def streamSomeBatches(ssc: StreamingContext) = {
    val data = Array(("bla1", "blubb1", "org", "Tokio"),
                    ("bla1", "blubb2", "com", "Johannisburg"),
                    ("bla2", "blubb1", "net", "Rio"))
    (0 until 40).foreach { _ =>
      queue += ssc.sparkContext.parallelize(data)
    }
  }

  /**
   * execute batch function
   */
  def batch(config: Config) = {
    logger.info(s"Using Batch mode.")
    val syncTable = new OnlineBatchSyncCassandra(config.cassandraHost.getOrElse("127.0.0.1"))
    val jobInfo = new CassandraJobInfo("Stock")
    if(syncTable.startNextBatchJob(jobInfo).isSuccess) {
      val sc = setupSparkBatchConfig(config.master)()
      val batchJob = new BatchJob(sc, jobInfo)
      batchJob.batchAggregate()
    } else {
      logger.error("Batch table is locked.") 
    }
  }

  /**
   * execute streaming function
   */
  def stream(config: Config) = {
    logger.info(s"Using HDFS-URL=${config.hdfsDStreamURL} and Kafka-URL=${config.kafkaDStreamURL}")
    val syncTable = new OnlineBatchSyncCassandra(config.cassandraHost.getOrElse("127.0.0.1"))
    val jobInfo = new CassandraJobInfo("Stock")
    if(syncTable.startNextOnlineJob(jobInfo).isSuccess) {
      val ssc = StreamingContext.getOrCreate(config.checkpointPath, setupSparkStreamingConfig(config.master, config.seconds))
      ssc.checkpoint(config.checkpointPath)
      val streamingJob = new StreamingJob(ssc, jobInfo)
      val dstream = streamSetup(ssc, streamingJob, config)
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
    val time = new Column[Long]("time")(scray.cassandra.sync.CassandraImplementation.lngType)
    val name = new Column[String]("name")(scray.cassandra.sync.CassandraImplementation.strType)
    val sum = new Column[Int]("sum")(scray.cassandra.sync.CassandraImplementation.intType)
         
    override val columns = time :: name :: sum :: Nil
    override val primaryKey = s"(${time.name})"
    override val indexes = None
  }

  def main(args : Array[String]) = {
    println("......................................")
    Options.parse(args) match {
      case Some(config) =>
              println("///////////////////////////////////////")

       val syncTable = new OnlineBatchSyncCassandra(config.cassandraHost.getOrElse("127.0.0.1"))
       val jobInfo = new CassandraJobInfo("Stock")
       syncTable.initJob(jobInfo, ExampleTable)

        config.batch match {
          case true =>  batch(config)
          case false => stream(config)
        }
      case None =>
              println("'''''''''''''''''''''''")

        // do nothing but displaying some help message
    }
  }

}
