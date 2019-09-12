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

package scray.hdfs.io.configure

import org.scalatest.BeforeAndAfter
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.WordSpec
import org.junit.Assert
import java.util.Arrays
import scray.hdfs.io.coordination.CloseFileTimer
import java.util.Timer
import org.apache.hadoop.io.SequenceFile

class CoordinatedWriterConfSpecs extends WordSpec with BeforeAndAfter with LazyLogging {
  "CoordinatedWriterConf " should {
    " create configuration object " in {

      val builder = new WriteParameter.Builder

      val config: WriteParameter = builder.setFileNameCreator(new FixNameCreator("file1")).createConfiguration

      Assert.assertEquals("file1", new FixNameCreator("file1").getNextFilename)
    }
    " compare different configurations " in {

      val config1 = (new WriteParameter.Builder)
      val config2 = (new WriteParameter.Builder)

      Assert.assertTrue(config1.createConfiguration ==  config2.createConfiguration)
      
      config1.setMaxFileSize(1024)
      Assert.assertFalse(config1.createConfiguration == config2.createConfiguration)
      Assert.assertFalse(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())

      
      config2.setMaxFileSize(1024)      
      Assert.assertTrue(config1.createConfiguration == config2.createConfiguration)
      Assert.assertTrue(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())

      
      config1.setFileNameCreator(new FixNameCreator("file1"))
      Assert.assertFalse(config1.createConfiguration == config2.createConfiguration)
      Assert.assertFalse(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())


      config2.setFileNameCreator(new FixNameCreator("file1"))
      Assert.assertTrue(config1.createConfiguration == config2.createConfiguration)
      Assert.assertTrue(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())


      config2.setFileNameCreator(new FixNameCreator("file2"))
      Assert.assertFalse(config1.createConfiguration == config2.createConfiguration)
      Assert.assertFalse(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())

      config1.setFileNameCreator(new FixNameCreator("file2"))
      Assert.assertTrue(config1.createConfiguration == config2.createConfiguration)
      Assert.assertTrue(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())
      
      config2.setStoreAsHiddenFileTillClosed(true)
      Assert.assertFalse(config1.createConfiguration == config2.createConfiguration)
      Assert.assertFalse(config1.createConfiguration.hashCode() === config2.createConfiguration.hashCode())
    }
  }
}