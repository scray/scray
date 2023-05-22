// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.sync.impl

import scray.sync.api.{VersionedData, VersionedDataApi}

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
import com.typesafe.scalalogging.LazyLogging

import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.InputStream
import java.util
import scala.io.Source

class FileVersionedDataApiImpl extends VersionedDataApi with LazyLogging {
  var versionInformations: HashMap[Int, VersionedData] = this.toMap(new ArrayList[VersionedData])
  val gson = new GsonBuilder().setPrettyPrinting().create();

  def getLatestVersion(dataSource: String, mergeKey: String): Option[VersionedData] = {
    versionInformations.get(scray.sync.api.VersionedData.createVersionKey(dataSource, mergeKey))
  }

  def updateVersion(dataSource: String, mergeKey: String, version: Long, data: String) {
    versionInformations.put(VersionedData.createVersionKey(dataSource, mergeKey), new VersionedData(dataSource, mergeKey, version, data))
  }

  def updateVersion(vd: VersionedData) {
    versionInformations.put(VersionedData.createVersionKey(vd.dataSource, vd.mergeKey), vd)
  }

  def persist(path: String): Unit = {
    this.writeToFile(path)
  }
  
  def persist(outStream: OutputStream): Unit = {
    this.writeToFile(outStream)
  }
  
  def load(path: String): Unit = {
    versionInformations = readFromFile(path)
  }
  
  def load(stream: InputStream): Unit = {
    versionInformations = readFromInputStram(stream)
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
  
    private def readFromInputStram(stream: InputStream): HashMap[Int, VersionedData] = {

    val dataList = () => {
      try {
        val fileContents = Source.fromInputStream(stream).getLines.mkString

        val jsonParser = new GsonBuilder().create();
        val listType = new TypeToken[ArrayList[VersionedData]]() {}.getType();

        gson.fromJson(fileContents, listType)
      } catch {
        case _ => {
          logger.debug(s"Unable to read from file. Use empty verson collection")
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

  private def toList(versionInformations: HashMap[Int, VersionedData]): java.util.List[VersionedData] = {
    versionInformations.keySet.map(key => {
      versionInformations.get(key).get
    }).toList.asJava
  }

  private def writeToFile(path: String): Unit = {
    val jsonString = gson.toJson(toList(versionInformations))

    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(jsonString)
    bw.close()
  }
  
  private def writeToFile(outStream: OutputStream): Unit = {
    val jsonString = gson.toJson(toList(versionInformations))

    val bw = new OutputStreamWriter(outStream)
    bw.write(jsonString)
    bw.flush()
    bw.close()
  }

  /**
   * Get all resources where a version exits for
   */
  override def getAllVersionedResources(): util.List[VersionedData] = {
    this.toList(this.versionInformations);
  }
}