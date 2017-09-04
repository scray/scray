package org.scray.example

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.scray.example.data.AggregationKey
import org.apache.spark.rdd.RDD
import scray.cassandra.sync.CassandraImplementation._
import scray.querying.sync.OnlineBatchSync
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.JobInfo
import scala.util.Failure
import scala.util.Success
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement

/**
 * Class containing all the batch stuff
 */
class BatchJob(@transient val sc: SparkContext, jobInfo: JobInfo[Statement, Insert, ResultSet]) extends LazyLogging with Serializable {
  println(sc.getConf.get("spark.cassandra.connection.host"))
  val syncTable = new OnlineBatchSyncCassandra(sc.getConf.get("spark.cassandra.connection.host"))

  /**
   * get the intial rdd to load data from
   */
  def getBatchRDD() = {
    // TODO: create rdd here
    // example to load data from Cassandra: sc.cassandraTable("scraykeyspace", "inputcolumnfamily")
    // for now, we use a hard-wired collection
    val data = Array(("bla1", "blubb1", "org", "Tokio"),
                    ("bla1", "blubb2", "com", "Johannisburg"),
                    ("bla2", "blubb1", "net", "Rio"))
    sc.parallelize(data)
  }

  /**
   * write resulting RDD into some storage or trigger external side effects
   */
  def writeBatchRDD(data: RDD[(AggregationKey, Long)]) = {
    // TODO: define place to write data to
    data.
      map(x => StreamingJob.saveDataMap(x)).
      // example howto save into Cassandra: saveToCassandra(StreamingJob.keyspace, StreamingJob.tablebatch)
      foreach(x => println(x))
    syncTable.completeBatchJob(jobInfo) match {
      case Success(u) => logger.info("Job marked as completed")
      case Failure(ex) => logger.error(s"Error while completing job ${ex.getMessage}")
    }
  }

  /**
   * do the job
   */
  def batchAggregate() = {
    // TODO: define your own job!
    val dataRDD = getBatchRDD.map { row => 
      (StreamingJob.buildAggregationKey(row), 1L) }.collect { case a if a._1.isDefined => (a._1.get, a._2) }
    val reducedRDD = dataRDD.reduceByKey(_ + _)
    writeBatchRDD(reducedRDD)
  }
}