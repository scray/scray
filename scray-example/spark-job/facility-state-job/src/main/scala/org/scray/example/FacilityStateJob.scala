package org.scray.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.scray.example.cli.Options

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
import org.scray.example.input.FacilityDataSources
import org.scray.example.conf.TEXT
import org.scray.example.conf.CASSANDRA
import scray.jdbc.sync.JDBCJobInfo
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.jdbc.sync.JDBCDbSession
import java.sql.PreparedStatement
import java.sql.ResultSet
import org.scray.example.input.KafkaOffsetReader
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.common.TopicPartition
import scala.util.Try
import org.apache.spark.storage.StorageLevel
import scala.util.Success
import scala.util.Failure
import org.scray.example.conf.KAFKA

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
    val conf = new SparkConf()
      .setAppName("Batch_" + this.getClass.getName)
      .setMaster(config.sparkMaster)
      .set("spark.ui.port", "8088")
      .set("spark.cassandra.connection.host", config.cassandraSeed)

    val sc = new SparkContext(conf)

    // Read configuration file
    val configuration = config.confFilePath match {
      case Some(confFilePath) => (new ConfigurationReader(confFilePath)).readConfigruationFile
      case None               => (new ConfigurationReader).readConfigruationFile
    }
    logger.info(s"Job configuration parameters: ${configuration}")

    val jobInfo = JDBCJobInfo(configuration.jobName, configuration.numberOfBatchVersions, configuration.numberOfBatchVersions)

    JDBCDbSession.getNewJDBCDbSession(configuration.syncJdbcURL, configuration.syncJdbcUsr, configuration.syncJdbcPw)
    .map(new OnlineBatchSyncJDBC(_))
    .map { syncApi =>
      syncApi.startNextBatchJob(jobInfo)

      // Get data source
      val dataSource = configuration.batchDataSource match {
        case TEXT      => FacilityDataSources.getFacilityFromTextFile(sc, configuration.batchFilePath)
        case CASSANDRA => FacilityDataSources.getFacilityFromCassandraDb(sc, configuration.cassandraKeyspace, configuration.cassandraTable)
        case KAFKA     => FacilityDataSources.getKafkaFacilitySource(sc,  configuration.kafkaTopic, Array(), configuration.kafkaBootstrapServers, syncApi, jobInfo)
      }
  
      if(dataSource.isDefined) {
        val job = new BatchJob(dataSource.get, configuration)
        job.run
      } else {
        logger.error("No valid data source found. Job will not be started")
      }
    }
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
    
   val jobInfo = JDBCJobInfo(config.jobName, config.numberOfBatchVersions, config.numberOfBatchVersions)


    

    if (cliParams.useSparkSQLJob) {
      val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
      val job = new SparkSQLStreamingJob(spark, config)
      job.run(config.windowStartTime)
    } else {
      this.startDistanceBasedAggregationJob(cliParams, config, jobInfo)
    }
  }

  def startDistanceBasedAggregationJob(cliConf: CliParameters, config: JobParameter, jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet]) = {

    logger.info(s"Using HDFS-URL=${config.checkpointPath} and Kafka-URL=${config.kafkaBootstrapServers}")
    val ssc = StreamingContext.getOrCreate(config.checkpointPath, setupSparkStreamingConfig(config.sparkMaster, config.sparkStreamingBatchSize))
    ssc.checkpoint(config.checkpointPath + "_" + System.currentTimeMillis())

    getAndPersistCurrentKafkaOffsets(jobInfo, config).map{kafkaOffsets => 
      val dstream = StreamingDStreams.getKafkaStringSource(ssc, Some(config.kafkaBootstrapServers), Some(config.kafkaTopic), StorageLevel.MEMORY_ONLY, kafkaOffsets)
  
      val job = new StreamingJob(ssc, config)
      job.runTuple(dstream.getOrElse(throw new RuntimeException("No stream to get data")))
  
    } match {
      case Success(e) => ssc.start(); ssc.awaitTermination()
      case Failure(ex) => logger.error(s"Error while preparing straming job. Exception: ${ex}\n ${ex.printStackTrace()}")
    }
  }

  def setupSparkStreamingConfig(masterURL: String, seconds: Int): () => StreamingContext = () => {
    val sparkConf = new SparkConf()
      .setAppName("Stream: " + this.getClass.getName)
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.backpressure.initialRate",  "20000")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")
      .set("spark.streaming.receiver.maxRate", "20000")
      .setMaster(masterURL)
    new StreamingContext(sparkConf, Seconds(seconds))
  }
  
  def getAndPersistCurrentKafkaOffsets(jobInfo:  JobInfo[PreparedStatement, PreparedStatement, ResultSet], config: JobParameter): Try[Map[TopicPartition, Long]] = {
    
    // Read and persist Kafka state
    JDBCDbSession.getNewJDBCDbSession(config.syncJdbcURL, config.syncJdbcUsr, config.syncJdbcPw)
      .map(new OnlineBatchSyncJDBC(_))
      .map { syncApi =>

        val offsetReader = new KafkaOffsetReader(config.kafkaBootstrapServers)
        val kafkaOffsets = offsetReader.getCurrentKafkaHighestOffsets(config.kafkaTopic)
        
        syncApi.initJobIfNotExists(jobInfo)
        syncApi.startNextOnlineJob(jobInfo)
        
        val offsetRanges = kafkaOffsets.foldLeft(Map[TopicPartition,Long]()) { (offsetRanges, startPoint) =>  
          
          logger.error(s"Persist Kafka start point ${startPoint} for job ${jobInfo}")
          startPoint.toJsonString.map{ startPoint =>
            syncApi.setOnlineStartPoint(jobInfo, System.currentTimeMillis(), startPoint) 
          }
          .flatten match {
            case Success(e) => logger.debug(s"Stored start possitions in database. Possition ${startPoint} for job ${jobInfo}")
            case Failure(e) => logger.warn(s"Error while writing start points to database. Exception ${e}")
          }
          
          offsetRanges + ((new TopicPartition(startPoint.topic, startPoint.partition), startPoint.offset))
        }
        
        offsetRanges
    }    
  }
  
  def getStremingKafkaStartOffset(jobInfo:  JobInfo[PreparedStatement, PreparedStatement, ResultSet], config: JobParameter) = {

  }
}
