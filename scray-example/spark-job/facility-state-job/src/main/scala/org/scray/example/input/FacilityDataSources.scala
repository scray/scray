package org.scray.example.input

import org.apache.spark.rdd.RDD
import org.scray.example.data.Facility
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import java.util.Calendar
import org.scray.example.data.JsonFacilityParser


object FacilityDataSources {
  def getFacilityFromCassandraDb(sc: SparkContext, keyspace: String, table: String): RDD[Facility[Long]] = {
    sc.cassandraTable(keyspace, table).map { row => {
        Facility(row.getString("type"), row.getString("state"), row.getLong("time"))
      }
    } 
  }
  
  @transient lazy val jsonParser = new JsonFacilityParser

  def getFacilityFromTextFile(sc: SparkContext, filePath: String): RDD[Facility[Long]] = {
    sc.textFile(filePath)
      .map(jsonParser.parse)
      .flatMap(x => x)
  }
}