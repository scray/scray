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
package scray.querying.sync

import scray.querying.sync.conf.SyncConfiguration
import org.scalatest.WordSpec
import scray.querying.sync.conf.SyncConfigurationLoader
import org.junit.Assert

class SyncConfigurationSpecs extends WordSpec { 
  
  "SyncConfiguration" should {
    "load configuration from file" in {
      val confLoader = SyncConfigurationLoader
      val conf = confLoader.loadConfig

      Assert.assertEquals(conf.dbSystem, "scray")
      Assert.assertEquals(conf.tableName, "synctable")
      Assert.assertEquals(conf.replicationSetting, "{'class': 'NetworkTopologyStrategy', 'DC1': '5', 'DC2': '3', 'DC3': '0'}")
    }
  }
  
}