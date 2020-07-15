package com.seeburger.research.cloud.ai

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.util.Failure
import scala.util.Success
import com.seeburger.research.cloud.ai.data.AggregationKey
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import scray.sync.impl.FileVersionedDataApiImpl
import scray.hdfs.io.osgi.ReadServiceImpl
import scray.hdfs.io.osgi.WriteServiceImpl
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.Decimal
import com.typesafe.scalalogging.LazyLogging

/**
 * Class containing all the batch stuff
 */
class BatchJob(@transient val sc: SparkContext, sqlUser: String, sqlPassword: String) extends LazyLogging with Serializable {
  val baseOutPath = "hdfs://hdfs.example.com/sql_data/"
  val dataOutPath = baseOutPath + System.currentTimeMillis()

  def batchAggregate() = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    this.getStartDate(baseOutPath + "request_sync.json")
    val sqlContext = new SQLContext(sc)
    val dataSource = sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:" + sqlUser + "/" + sqlPassword + "@//cls42:1521/pcomsilsrv")
      .option("dbtable", "TABLE1.FF")
      .option("user", sqlUser)
      .option("password", sqlPassword)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load
      .select("*").where($"CSTARTTIME" > this.getStartDate(baseOutPath + "request_sync.json"))
      .write
      .parquet(dataOutPath)

    val maxTime = sqlContext.read.parquet(dataOutPath)
      .select(max("CSTARTTIME"))
      .head
      .getAs(0)
      .toString
      .toLong

    this.perstistLastStartDate(baseOutPath + "request_sync.json", maxTime)
  }

  def getStartDate(storragePath: String): Long = {
    val versionReader = new FileVersionedDataApiImpl
    val readService = new ReadServiceImpl

    try {
      val inStream = readService.getInputStream(storragePath, "hdfs", "".getBytes).get
      versionReader.load(inStream)
    } catch {
      case e: Throwable => println("Unable to read verion file. Use default value" + e.getMessage)
    }

    val latestVersion = versionReader.getLatestVersion("http://com.seeburger.research/resource/cloudai/sql-sql-data/version", "_") match {
      case None             => 0
      case Some(oldVersion) => oldVersion.data.toLong
    }

    latestVersion
  }

  def perstistLastStartDate(destinationPath: String, date: Long) {
    val versionWriter = new FileVersionedDataApiImpl

    versionWriter.updateVersion("http://com.seeburger.research/resource/cloudai/sql-sql-data/version", "_", System.currentTimeMillis(), date.toString)

    try {
      val outStream = new WriteServiceImpl().writeRawFile(destinationPath, "hdfs", "".getBytes)
      versionWriter.persist(outStream)
    } catch {
      case e: Throwable => println("Unable to write verion file. Last state will not be persisted " + e.printStackTrace())
    }

  }
}
