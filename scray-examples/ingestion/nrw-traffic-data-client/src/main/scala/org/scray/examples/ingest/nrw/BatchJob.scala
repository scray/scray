package org.scray.examples.ingest.nrw

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.util.Failure
import scala.util.Success
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import org.scray.examples.ingest.nrw.data.AggregationKey

/**
 * Class containing all the batch stuff
 */
class BatchJob(@transient val sc: SparkContext) extends LazyLogging with Serializable {

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
   * do the job
   */
  def batchAggregate() = {
    // TODO: define your own job!
    val dataRDD = getBatchRDD.map(row => ((row._1), 1))
    val reducedRDD = dataRDD.reduceByKey(_ + _)
    .map(xx => println("Number of rows: " + xx))
  }
}
