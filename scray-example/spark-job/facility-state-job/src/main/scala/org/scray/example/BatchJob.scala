package org.scray.example

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark._
import scray.cassandra.sync.CassandraImplementation._
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.JobInfo
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.typesafe.scalalogging.LazyLogging

/**
 * Class containing all the batch stuff
 */
class BatchJob(@transient val sc: SparkContext, jobInfo: JobInfo[Statement, Insert, ResultSet]) extends LazyLogging with Serializable {
  println(sc.getConf.get("spark.cassandra.connection.host"))
  val syncTable = new OnlineBatchSyncCassandra(sc.getConf.get("spark.cassandra.connection.host"))

  /**
   * do the job
   */
  def batchAggregate() = {

  }
}