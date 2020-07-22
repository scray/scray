package com.seeburger.research.cloud.ai

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import scala.collection.mutable.Queue
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.seeburger.research.cloud.ai.cli.Options
import com.seeburger.research.cloud.ai.cli.Config
import com.typesafe.scalalogging.LazyLogging

/**
 * @author <author@name.org>
 */
object IngestSlaSqlData extends LazyLogging {
  

  
  def setupSparkBatchConfig(masterURL: String): () => SparkContext = () => {
    logger.info(s"Connecting batch context to Spark on $masterURL")
    new SparkContext(new SparkConf().setAppName("Batch: " + this.getClass.getName).setMaster(masterURL))
  }

  val queue = new Queue[RDD[String]]()



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
      val sc = setupSparkBatchConfig(config.master)()
      val batchJob = new BatchJob(sc, config.sqlUser, config.sqlPassword)
      batchJob.batchAggregate()
  }



  def main(args : Array[String]): Unit = {
   Options.parse(args) match {
      case Some(config) =>

        config.batch match {
          case true =>  batch(config)
          case false => batch(config)
        }
      case None => {
        println("Error while parsing command line parameters")
        Options.parse(args)
      }
    }
  }

}
