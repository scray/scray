package scray.sync.impl

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
import scray.sync.api.VersionedDataApi
import com.typesafe.scalalogging.LazyLogging

class FileVersionedDataApiImpl(storragePath: String) extends VersionedDataApi with LazyLogging {
  var versionInformations: HashMap[Int, VersionedData] = readFromFile(storragePath)
  val gson = new GsonBuilder().setPrettyPrinting().create();

  def getLatestVersion(dataSource: String, mergeKey: String): Option[VersionedData] = {
    versionInformations.get(scray.sync.api.VersionedData.createVersionKey(dataSource, mergeKey))
  }

  def updateVersion(dataSource: String, mergeKey: String, version: Long, data: String) {
    versionInformations.put(VersionedData.createVersionKey(dataSource, mergeKey), new VersionedData(dataSource, mergeKey, version, data))
  }

  def flush = {
    this.writeToFile(storragePath)
  }

  private def readFromFile(path: String): HashMap[Int, VersionedData] = {

    val dataList = () => {
      try {
        val fileContents = Source.fromFile(path).getLines.mkString

        val jsonParser = new GsonBuilder().create();
        val listType = new TypeToken[ArrayList[VersionedData]]() {}.getType();

        gson.fromJson(fileContents, listType)
      } catch {
        case _ => {
          logger.debug(s"Unable to open file ${path}. Use empty verson collection")
          new ArrayList[VersionedData]
        }
      }
    }
    toMap(dataList())
  }

  private def toMap(dataList: java.util.List[VersionedData]) = {
    dataList.asScala.foldLeft(new HashMap[Int, VersionedData])((acc, nexVersiondata) => {
      val keyExists = acc.get(nexVersiondata.getVersionKey) match {
        // Add data if not exists
        case None => acc.put(nexVersiondata.getVersionKey, nexVersiondata)
        case Some(existingVersion) => {
          if (existingVersion.version > nexVersiondata.version) {
            logger.debug(s"Existing version is newer than next version. Existing: ${existingVersion}, Next: ${nexVersiondata}")
          } else {
            acc.put(nexVersiondata.getVersionKey, nexVersiondata)
          }
        }
      }
      acc
    })
  }

  private def toList(versionInformations: HashMap[Int, VersionedData]): List[VersionedData] = {
    versionInformations.keySet.map(key => {
      versionInformations.get(key).get
    }).toList
  }

  private def writeToFile(path: String) = {
    val jsonString = gson.toJson(toList(versionInformations))

    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(jsonString)
    bw.close()
  }
}