package scray.querying.storeabstraction

import com.twitter.storehaus.QueryableStore
import scray.querying.description.{ TableIdentifier, VersioningConfiguration }
import scray.querying.description.Row

/**
 * interface for store generation
 */
trait StoreGenerators {
  
  /**
   * creates a row store, i.e. a store that has a primary key column and maybe a bunch of other columns  
   */
  def createRowStore(table: TableIdentifier): 
    Option[(QueryableStore[_, _], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]]))]

  /**
   * gets the extractor, that helps to evaluate meta-data for this type of dbms
   */
  def getExtractor[S <: QueryableStore[_, _]](store: S, tableName: Option[String], versions: Option[VersioningConfiguration[_, _]], dbSystem: Option[String]): StoreExtractor[S]
}