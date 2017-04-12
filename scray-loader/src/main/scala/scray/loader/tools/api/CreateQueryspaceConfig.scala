package scray.loader.tools.api

import scray.querying.description.TableIdentifier

trait CreateQueryspaceConfig {
  def getConfig(tis: List[TableIdentifier]): String
  def getTableEntry(ti: TableIdentifier): String
  
}