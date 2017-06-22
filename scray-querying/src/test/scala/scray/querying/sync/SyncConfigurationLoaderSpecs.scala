package scray.querying.sync

import org.scalatest.WordSpec
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scray.querying.sync.conf.SyncConfiguration


class SyncConfigurationLoader extends WordSpec {
  
  "SyncConfigurationLoader" should {
    " pars text " in {
      val text = "useLightweightTransactions: false"
      
      val yaml = new Yaml(new Constructor(classOf[SyncConfiguration]))
      val e = yaml.load(text).asInstanceOf[SyncConfiguration]
    }
   }
  
}