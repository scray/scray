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
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.junit.Assert
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File

class IndexFileRecordSpecs  extends WordSpec with LazyLogging {
  
  "Index record " should {
    " be created from simple attributes " in {
      val key = "abc".getBytes("UTF8")
      val value = "data".getBytes
      
      val record = IndexFileRecord(key, 0, value.length)
      
      // create byte representation
      val recordInBytes = record.getByteRepresentation
      val is: DataInputStream = new DataInputStream(new ByteArrayInputStream(recordInBytes));
      
      // create record from byte representation
      val recordNew = IndexFileRecord(is)
      
      Assert.assertEquals("abc", new String(recordNew.getKey))
    }
//    "print keys" in {
//      val fileInputStream = new FileInputStream(new File("/home/stefan/Downloads/tsilsilerrorblobs-1494510423171(1).idx"))
//      val is = new DataInputStream(new BufferedInputStream(fileInputStream, 2000000))
//      
//      val bytes = new Array[Byte](4)
//      is.read(bytes)
//      
//      for(i <- 0 to 100000000) {
//        val idxRecord = IndexFileRecord(is)
//        println(i)
//        println(new String(idxRecord.getKey))
//      }
//    }
  }
}