package scray.jdbc.automation

import scray.querying.storeabstraction.StoreGenerators
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.description.VersioningConfiguration
import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import com.twitter.util.FuturePool
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.Row

class JDBCStoreGenerators extends StoreGenerators {
  
  /**
   * creates a row store, i.e. a store that has a primary key column and maybe a bunch of other columns
   */
  def createRowStore[Q <: DomainQuery](table: TableIdentifier): Option[(QueryableStoreSource[Q], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]]))]

  /**
   * gets the extractor, that helps to evaluate meta-data for this type of dbms
   */
  def getExtractor[Q <: DomainQuery, S <: QueryableStoreSource[Q]](
      store: S, tableName: Option[String], versions: Option[VersioningConfiguration[_, _]], 
      dbSystem: Option[String], futurePool: FuturePool): StoreExtractor[S]

 
}
