package ${package}

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark._
import ${package}.data.AggregationKey
import org.apache.spark.rdd.RDD

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
   * write resulting RDD into some storage or trigger external side effects
   */
  def writeBatchRDD(data: RDD[(AggregationKey, Long)]) = {
    // TODO: define place to write data to
    data.
      map(x => StreamingJob.saveDataMap(x)).
      // example howto save into Cassandra: saveToCassandra(StreamingJob.keyspace, StreamingJob.tablebatch)
      foreach(x => println(x))
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