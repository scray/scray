package scray.querying.storeabstraction

import scray.querying.description.{ TableIdentifier, VersioningConfiguration }
import scray.querying.description.Row
import scray.querying.source.store.QueryableStoreSource
import scray.querying.queries.DomainQuery
import com.twitter.util.FuturePool

/**
 * interface for store generation
 */
trait StoreGenerators {

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