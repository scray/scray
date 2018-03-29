// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.hdfs.index.format.example

import scray.hdfs.index.format.sequence.SequenceFileWriter
import scray.hdfs.index.format.orc.ORCFileWriter

object WriteExampleOrcFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

      val writer = new ORCFileWriter(s"${args(0)}/scray-data.orc}")

      for (i <- 100 to 200) {
        val key = "key_" + i
        val value = "data_" + i

        println(s"Write key value data. key=${key}, value=${value}")

        writer.insert(key, System.currentTimeMillis(), value.getBytes)
      }

      writer.close
    }
  }
}