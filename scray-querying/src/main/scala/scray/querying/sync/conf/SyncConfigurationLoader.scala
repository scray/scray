package scray.querying.sync.conf


class SyncConfigurationLoader {

  private val DEFAULT_CONFIGURATION = "scray-sync.yaml";
  
  def loadConfig: SyncConfiguration = {
    
    //val yaml = new Yaml()
    
    new SyncConfiguration
  }
}