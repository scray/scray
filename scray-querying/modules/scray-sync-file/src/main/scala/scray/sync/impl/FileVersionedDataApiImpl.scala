package scray.sync.impl

import scray.sync.api.VersionedDataApi
import scray.sync.api.VersionedData
import scala.io.Source
import scala.collection.mutable.HashMap
import java.util.ArrayList
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import com.google.gson.reflect.TypeToken
import scray.sync.api.VersionedData
import collection.JavaConverters._

class FileVersionedDataApiImpl(storragePath: String) extends VersionedDataApi {
  val versions: java.util.List[VersionedData] = new ArrayList[VersionedData]
  val gson = new GsonBuilder().setPrettyPrinting().create();

  def getLatestVersion(dataSource: String, mergeKey: String): VersionedData = {

    val vs1 = readFromFile("/tmp/ibm")
    
    vs1.asScala.map(println)

    vs1.get(0)
  }

  def writeNextVersion(dataSource: String, mergeKey: String, version: Long, data: String) {
    writeToFile("/tmp/ibm")
  }

  private def readFromFile(path: String): java.util.List[VersionedData] = {
    val fileContents = Source.fromFile(path).getLines.mkString

    val jsonParser = new GsonBuilder().create();
    val listType = new TypeToken[ArrayList[VersionedData]](){}.getType();
    
    jsonParser.fromJson(fileContents, listType)
  }

  private def writeToFile(path: String) = {
    val vs1 = new VersionedData("http://ibmff", "key1", 2, "hdfs://ibm1.com")
    versions.add(vs1)
    val vs2 = new VersionedData("http://ibmff", "key2", 3, "hdfs://ibm2.com")
    versions.add(vs2)

    val jsonString = gson.toJson(versions)

    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(jsonString)
    bw.close()

  }
}