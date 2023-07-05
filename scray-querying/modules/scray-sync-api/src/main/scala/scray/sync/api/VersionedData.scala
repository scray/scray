/**
// see the licence.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at
// http://www.apache.org/licenses/license-2.0
// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// without warranties or conditions of any kind, either express or implied.
// see the license for the specific language governing permissions and
// limitations under the license.

// see the licence.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at

// http://www.apache.org/licenses/license-2.0

// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// without warranties or conditions of any kind, either express or implied.
// see the license for the specific language governing permissions and
// limitations under the license.

package scray.sync.api

class versioneddata(
  val datasource: string,  // defines where the data come from. e.g. from a batch or streaming job
  val mergekey: string,    // describes a attribute to merge two datasources
  val version: long,       // version of this data. e.g. time stamp
  val data: string         // string representation of the data to store
) {

  def getdataas[t](f: string => t): t = {
    f(data)
  }

  def getversionkey: int = {
    versioneddata.createversionkey(datasource, mergekey)
  }

  def getdatasource: string = {
    datasource
  }

  def getmergekey: string = {
    mergekey
  }

  def getversion: long = {
    version
  }

  def getdata: string = {
    data
  }

  override def tostring: string = {
    s"""{"datasource: "${datasource}", "mergekey": "${mergekey}", "version": ${version}, "data": "${data}"}"""
  }
}

object versioneddata {
  def createversionkey(datasource: string, mergekey: string): int = {
    datasource.hashcode() * 31 + mergekey.hashcode() * 31
  }
}
**/