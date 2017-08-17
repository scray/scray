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
package scray.hdfs.index.format

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import java.io.FileInputStream
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.File
import scala.collection.mutable.MutableList
import org.junit.Assert

class IndexFileSpecs extends WordSpec with LazyLogging {
    "Index file " should {
      " write index records to file" in {
        
        val records = new MutableList[IndexFileRecord] 
        
        for(i <- 0 to 10) {
          val key = s"key${i}".getBytes("UTF8")
          val value = s"val${i}".getBytes("UTF8")
          
          records += IndexFileRecord(key, 0, value.length)
        }
        
        val idxFile = IndexFile.apply.writeIndexFile("target/testIdxFile.idx", records.toList)
      }
      " be read from file " in {
          val fileInputStream = new FileInputStream(new File("target/testIdxFile.idx"))
          val is = new DataInputStream(new BufferedInputStream(fileInputStream, 2000000))
          
          val indexFile = IndexFile.apply.getReader(is)
          
          while(indexFile.hasNextRecord) {
            println(new String(indexFile.nextRecord.get.getKey))
            println((indexFile.nextRecord.get.getStartPosition))
            println(indexFile.nextRecord.get.getValueLength)
          }
      }
    }
}