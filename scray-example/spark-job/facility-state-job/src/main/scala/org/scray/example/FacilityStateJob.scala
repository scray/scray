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

  def main(args: Array[String]): Unit = {
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

  /**
   * execute batch function
   */
  def batch(config: CliParameters) = {
    val conf = new SparkConf().setAppName("Batch_" + this.getClass.getName).setMaster(config.sparkMaster).set("spark.ui.port", "8088").set("hadoop.home.dir", "/tmp")
    val sc = new SparkContext(conf)

    val configuration = config.confFilePath match {
      case Some(confFilePath) => (new ConfigurationReader(confFilePath)).readConfigruationFile
      case None               => (new ConfigurationReader).readConfigruationFile
    }
    logger.info(s"Job configuration parameters: ${configuration}")

    val job = new BatchJob(sc, configuration)

    job.run
  }

  /**
   * execute streaming function
   */
  def stream(cliParams: CliParameters) = {

    val config = cliParams.confFilePath match {
      case Some(confFilePath) => (new ConfigurationReader(confFilePath)).readConfigruationFile
      case None               => (new ConfigurationReader).readConfigruationFile
    }
    logger.info(s"Job configuration parameters: ${config}")

    if (cliParams.useSparkSQLJob) {
      val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
      val job = new SparkSQLStreamingJob(spark, config)
      job.run(config.windowStartTime)
    } else {
      this.startDistanceBasedAggregationJob(cliParams, config)
    }
  }

  def startDistanceBasedAggregationJob(cliConf: CliParameters, config: JobParameter) = {

    logger.info(s"Using HDFS-URL=${config.checkpointPath} and Kafka-URL=${config.kafkaBootstrapServers}")
    val ssc = StreamingContext.getOrCreate(config.checkpointPath, setupSparkStreamingConfig(config.sparkMaster, config.sparkStreamingBatchSize))
    ssc.checkpoint(config.checkpointPath + "_" + System.currentTimeMillis())
    val dstream = StreamingDStreams.getKafkaStringSource(ssc, Some(config.kafkaBootstrapServers), Some(config.kafkaTopic))

    val job = new StreamingJob(ssc, config)
    job.runTuple(dstream.getOrElse(throw new RuntimeException("No stream to get data")))

    ssc.start()
    ssc.awaitTermination()
  }

  def setupSparkStreamingConfig(masterURL: String, seconds: Int): () => StreamingContext = () => {
    val conf = new SparkConf().setAppName("Stream: " + this.getClass.getName).setMaster(masterURL)
    new StreamingContext(conf, Seconds(seconds))
  }
}
