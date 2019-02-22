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

class CoordinatedWriterConfSpecs extends WordSpec with BeforeAndAfter with LazyLogging {
   "CoordinatedWriterConf " should {
    " create configuration object " in {
      
      val builder = new WriteParameter.Builder
      
      val config: WriteParameter = builder.setFileNameCreator(new FixNameCreator("file1")).createConfiguration
      
      Assert.assertEquals("file1", new FixNameCreator("file1").getNextFilename)
    }
   }
}