package scray.loader.tools

import scray.loader.tools.api.CreateQueryspaceConfig
import scray.querying.description.TableIdentifier
import java.util.Collection
import scala.collection.JavaConverters._

/**
 * Create content of a queryspace configuration file.
 */
class QueryspaceConfigCreatorImpl(queryspaceName: String, version: Int) extends CreateQueryspaceConfig {
  
  def getHeader = s"name ${queryspaceName} version ${version}\n"
  
  override def getTableEntry(ti: TableIdentifier): String = {
     s"""table { ${ti.dbSystem}, "${ti.dbId}", "${ti.tableId}" }\n"""
  }
  
  override def getConfig(tis: List[TableIdentifier]): String = {
    tis.foldLeft(getHeader)((configuration, newEntry) => configuration + getTableEntry(newEntry))
  }
}