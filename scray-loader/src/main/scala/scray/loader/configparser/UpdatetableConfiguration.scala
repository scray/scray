package scray.loader.configparser

import scray.querying.sync.types.DbSession

trait UpdatetableConfiguration {
  
  def updateConfiguration(config: ScrayConfiguration): Option[DbSession[_, _, _]]
  
}