package org.scray.examples.ingest.prometheus

import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.osgi.WriteServiceImpl
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import java.util.Date
import scray.sync.impl.FileVersionedDataApiImpl
import scray.hdfs.io.osgi.ReadServiceImpl
import com.typesafe.scalalogging.Logger
import org.scray.examples.ingest.prometheus.Client


object MetricsCrawler  {
  
  def main(args : Array[String]) {
    val client = new Client
   
    val prometheusMetriklistUrl = args(0)
    val destinationPath = args(1)

    var metricsList: List[String] = null 
    var currentApiRequest = getStartDate(destinationPath + "request_sync.json") 
    //currentApiRequest = 1568635494
    var nextApiRequest = 0L


    val hdfsUser = "hdfs"
    
    for(i <- 0 to Int.MaxValue) {
    
      // Update metrics list
      if(i % 10 == 0) {
        metricsList = client.getMetricsList(prometheusMetriklistUrl + "/label/__name__/values")
      }
      

      nextApiRequest = System.currentTimeMillis / 1000
      queryMetrics(prometheusMetriklistUrl, currentApiRequest, nextApiRequest, metricsList, destinationPath + currentApiRequest + "_")
      currentApiRequest = nextApiRequest

      println("Next api request\t" + nextApiRequest)
      
      perstistLastStartDate(destinationPath + "request_sync.json", nextApiRequest)
      
      // Query every 5 minutes
      Thread.sleep(60 * 5 *  1000)
    }
  }
  
  def queryMetrics(prometheusMetriklistUrl: String, dataStartDate: Long, dataEndDate: Long, matricsToQuery: List[String], outPath: String) = {
    val logger = Logger("Prometheus crawler")
    val client = new Client
    val writeService = new WriteServiceImpl
    
    val config = new WriteParameter.Builder()
    .setPath(outPath)
    .setFileFormat(SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT)
    .setSequenceFileCompressionType("BLOCK")
    .setUser("hdfs")
    .createConfiguration

    val writeId = writeService.createWriter(config)
    
    val numMetricsWritte = matricsToQuery.foldLeft(0)((acc, metric) => {

      val nextMetricQuery = prometheusMetriklistUrl + "/query_range?query=" + metric + "&start=" + dataStartDate + "&end=" + dataEndDate + "&step=30s"
      val metrics = client.getExtractedMetricValues(nextMetricQuery)
      
      logger.info("Query metric: " +  nextMetricQuery)
      
      val numMetricsWritte = metrics.foldLeft(0)((acc, metric) => {
        writeService.insert(writeId, dataStartDate.toString(), System.currentTimeMillis, metric.getBytes)    
        acc + 1
      })
      
      acc + numMetricsWritte 
    })
    
    writeService.close(writeId)
    println(new Date + " num metrics written " + numMetricsWritte)
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
    
    val latestVersion = versionReader.getLatestVersion("http://org.scray.examples/resource/prometheus-crawler/version", "_") match {
      case None => System.currentTimeMillis / 1000
      case Some(oldVersion) => oldVersion.data.toLong
    }
    
    latestVersion
  }
  
  def perstistLastStartDate(destinationPath: String, date: Long) {
    val versionWriter = new FileVersionedDataApiImpl
    
    versionWriter.updateVersion("http://org.scray.examples/resource/prometheus-crawler/version", "_", System.currentTimeMillis(), date.toString)
    
    try {
      val outStream = new WriteServiceImpl().writeRawFile(destinationPath, "hdfs", "".getBytes)
      versionWriter.persist(outStream)
    } catch {
      case e: Throwable => println("Unable to write verion file. Last state will not be persisted " + e.printStackTrace())
    }

  }

}
