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

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream

import org.scalatest.WordSpec

import junit.framework.Assert

class FileVersionedDataApiImplSpecs extends WordSpec {
  "FileVersionedData" should {
    "update versions " in {
      val fs = new FileVersionedDataApiImpl

      fs.updateVersion("online", "key", 1, "")
      Assert.assertEquals(fs.getLatestVersion("online", "key").get.version, 1)

      fs.updateVersion("online", "key", 2, "")
      Assert.assertEquals(fs.getLatestVersion("online", "key").get.version, 2)
    }
    "persist state to local file " in {
      
      // Persist example data
      val syncInstanceCreate = new FileVersionedDataApiImpl
      syncInstanceCreate.updateVersion("http://scray.org/resourc/sync/source/online", "date", 1, s"""{"date": "1234", "topic": "topic_01", "partition": 0, "offset": 4711}""")  
      syncInstanceCreate.updateVersion("http://scray.org/resourc/sync/source/batch",  "date", 1, s"""{"date": "1234", "file": "hdfs://hdfs.scray.org/test/1bw2CYTuNj.seq"}""")
      
      syncInstanceCreate.persist("target/FileVersionedDataApiImplSpecs_persist.json")
      
      // Read persisted data
      val syncInstanceRead = new FileVersionedDataApiImpl
      syncInstanceRead.load("target/FileVersionedDataApiImplSpecs_persist.json")
      
      Assert.assertEquals(syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/online", "date").get.version, 1)
      Assert.assertEquals(syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/batch",  "date").get.version, 1)
    }
    "persist state with Input/OutputStream " in {
      
      // Persist example data
      val syncInstanceCreate = new FileVersionedDataApiImpl
      syncInstanceCreate.updateVersion("http://scray.org/resourc/sync/source/online", "date", 1, s"""{"date": "1234", "topic": "topic_01", "partition": 0, "offset": 4711}""")  
      syncInstanceCreate.updateVersion("http://scray.org/resourc/sync/source/batch",  "date", 1, s"""{"date": "1234", "file": "hdfs://hdfs.scray.org/test/1bw2CYTuNj.seq"}""")
      
      val outStream = new FileOutputStream("target/FileVersionedDataApiImplSpecs_persist_stream.json")
      syncInstanceCreate.persist(outStream)
      
      // Read persisted data
      val syncInstanceRead = new FileVersionedDataApiImpl
      val inFile = new FileInputStream(new File("target/FileVersionedDataApiImplSpecs_persist_stream.json"))
      syncInstanceRead.load(inFile)
      
      Assert.assertEquals(syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/online", "date").get.version, 1)
      Assert.assertEquals(syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/batch",  "date").get.version, 1)
    }
  }
}