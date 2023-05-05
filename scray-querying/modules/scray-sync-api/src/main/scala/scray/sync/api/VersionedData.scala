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

package scray.sync.api

class VersionedData(
  val dataSource: String,  // Defines where the data come from. E.g. from a batch or streaming job
  val mergeKey: String,    // Describes a attribute to merge two dataSources
  val version: Long,       // Version of this data. E.g. time stamp
  val data: String         // String representation of the data to store
) {
  
  def getDataAs[T](f: String => T): T = {
    f(data)
  }

  def getVersionKey: Int = {
    VersionedData.createVersionKey(dataSource, mergeKey)
  }

  def getDataSource: String = {
    dataSource
  }

  def getMergeKey: String = {
    mergeKey
  }

  def getVersion: Long = {
    version
  }

  def getData: String = {
    data
  }

  override def toString: String = {
    s"""{"dataSource: "${dataSource}", "mergeKey": "${mergeKey}", "version": ${version}, "data": "${data}"}"""
  }
}

object VersionedData {
  def createVersionKey(dataSource: String, mergeKey: String): Int = {
    dataSource.hashCode() * 31 + mergeKey.hashCode() * 31
  }
}