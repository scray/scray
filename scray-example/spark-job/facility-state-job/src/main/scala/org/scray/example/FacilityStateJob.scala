package org.scray.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.scray.example.cli.Options

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.typesafe.scalalogging.Logger

import scray.cassandra.sync.CassandraJobInfo
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.querying.sync.JobInfo
import org.apache.spark.sql.SparkSession
import org.scray.example.conf.ConfigurationReader
import org.scray.example.conf.ConfigurationReader
import org.scray.example.conf.JobParameter
import org.scray.example.input.StartTimeReader
import org.scray.example.cli.CliParameters

object FacilityStateJob {

  val logger = Logger("org.scray.example.FacilityStateJob")
  /**
   * creates a Spark Streaming context
   */
  def setupSparkStreamingConfig(masterURL: String, seconds: Int, jobInfo: JobInfo[Statement, Insert, ResultSet], config: JobParameter): () => StreamingContext = () => {

    val conf = new SparkConf().setAppName("Stream: " + this.getClass.getName).setMaster(masterURL)
    val ssc = new StreamingContext(conf, Seconds(seconds))

    val streamingJob = new StreamingJob(ssc, jobInfo, config)
    streamSetup(ssc, streamingJob, config)

    ssc
  }

  def setupSparkBatchConfig(masterURL: String): () => SparkContext = () => {
    logger.info(s"Connecting batch context to Spark on $masterURL")
    new SparkContext(new SparkConf().setAppName("Batch: " + this.getClass.getName).setMaster(masterURL))
  }

  def streamSetup(ssc: StreamingContext, streamingJob: StreamingJob, config: JobParameter): Unit = {

    val dstream = StreamingDStreams.getKafkaStringSource(ssc, Some(config.kafkaBootstrapServers), Some(config.kafkaTopic))

    //example how to stream from Kafka
//    val dstream = config.kafkaDStreamURL.flatMap { url =>.
//        StreamingDStreams.getKafkaCasAxSource(ssc, config.kafkaDStreamURL, config.kafkaTopic).map(_.map(_._2)) }.getOrElse {
//          // shall fail if no stream source was provided on the command line
//          config.hdfsDStreamURL.flatMap { _ => StreamingDStreams.getTextStreamSource(ssc, config.hdfsDStreamURL) }.
//          map{_.map{ casaxStr =>
//            val bytes = casaxStr.getBytes("UTF-8")
//            decoder.fromBytes(bytes)}}.get
//    } 
    //val dstream = ssc.queueStream(queue)
    //dstream.checkpoint(Duration(config.checkpointDuration))
    streamingJob.runTuple(dstream.getOrElse(throw new RuntimeException("No stream to get data from")))
  }

  /**
   * execute batch function
   */
  def batch(config: CliParameters) = {
    //    logger.info(s"Using Batch mode.")
    //    val syncTable = new OnlineBatchSyncCassandra(config.cassandraHost.getOrElse("127.0.0.1"))
    //    val jobInfo = new CassandraJobInfo("facility-state-job", config.numberOfBatchVersions, config.numberOfOnlineVersions)
    //    if(syncTable.startNextBatchJob(jobInfo).isSuccess) {
    //      val sc = setupSparkBatchConfig(config.master)()
    //      val batchJob = new BatchJob(sc, jobInfo)
    //      batchJob.batchAggregate()
    //    } else {
    //      logger.error("Batch table is locked.") 
    //    }
  }

  /**
   * execute streaming function
   */
  def stream(config: CliParameters) = {

    //logger.error(s"Using HDFS-URL=${config.hdfsDStreamURL} and Kafka-URL=${config.kafkaDStreamURL}")
    //val syncTable = new OnlineBatchSyncCassandra(config.cassandraHost.getOrElse("127.0.0.1"))
    val jobInfo = new CassandraJobInfo("facility-state-job", 42, 42)
    //if(syncTable.startNextOnlineJob(jobInfo).isSuccess) {
    val configuration = config.confFilePath match {
      case Some(confFilePath) => (new ConfigurationReader(confFilePath)).readConfigruationFile
      case None               => (new ConfigurationReader).readConfigruationFile
    }

    val ssc = StreamingContext.getOrCreate(configuration.checkpointPath, setupSparkStreamingConfig(config.sparkMaster, 10, jobInfo, configuration))
    ssc.checkpoint(configuration.checkpointPath)
    val streamingJob = new StreamingJob(ssc, jobInfo, configuration)
    val dstream = streamSetup(ssc, streamingJob, configuration)
    //val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()

    logger.info(s"Job configuration parameters: ${configuration}")

    // prepare to checkpoint in order to use some state (updateStateByKey)
    ssc.start()
    // TODO: write out zk information for closing this app  
    // log out random port to connect to
    // TODO: remove code to insert data
    // streamSomeBatches(ssc)

    ssc.awaitTermination()
    //} else {
    //  logger.error("Streaming table locked")
    //}
  }

  //  object ExampleTable extends ArbitrarylyTypedRows {
  //    val time = new Column[Long]("time")
  //    val name = new Column[String]("name")
  //    val sum = new Column[Int]("sum")
  //         
  //    override val columns = time :: name :: sum :: Nil
  //    override val primaryKey = s"(${time.name})"
  //    override val indexes = None
  //  }

  def main(args: Array[String]) = {
    Options.parse(args) match {
      case Some(config) => {

        config.batch match {
          case true  => batch(config)
          case false => stream(config)
        }
      }
      case None => {
        println("Error while parsing command line parameters ")
        Options.parse(args)
      }
    }
  }

}
