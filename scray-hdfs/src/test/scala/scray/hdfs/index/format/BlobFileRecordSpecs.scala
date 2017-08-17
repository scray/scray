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

class BlobFileRecordSpecs  extends WordSpec with LazyLogging {
  
  "Blob record " should {
    " be created from simple attributes " in {
      val key = "abc".getBytes("UTF8")
      val value = "data".getBytes
      
      val blobRecord = BlobFileRecord(key,value)
      Assert.assertEquals("abc", new String(blobRecord.getKey))
      Assert.assertEquals("data", new String(blobRecord.getValue))

      // create byte representation
      val recordInBytes = blobRecord.getByteRepresentation
      val is: DataInputStream = new DataInputStream(new ByteArrayInputStream(recordInBytes));
      
      // create record from byte representation
      val recordNew = BlobFileRecord(is)
      
      Assert.assertEquals("abc", new String(recordNew.getKey))
      Assert.assertEquals("data", new String(recordNew.getValue))

    }
  }
}