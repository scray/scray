package scray.loader.configparser

trait UpdatetableConfiguration {
  
  def updateConfiguration(config: ScrayConfiguration): Unit
  
}