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