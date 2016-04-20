//package scray.loader
//
//import scray.querying.description.QueryspaceConfiguration
//import scray.querying.description.TableConfiguration
//import scray.querying.description.internal.MaterializedView
//import scray.querying.queries.DomainQuery
//import scray.querying.description.ColumnConfiguration
//import com.typesafe.scalalogging.slf4j.LazyLogging
//
//
///**
// * a generic query space that can be used to load tables from
// * various different databases 
// */
//class ScrayLoaderQuerySpace(name: String) extends QueryspaceConfiguration(name) with LazyLogging {
//  
//  /**
//   * if this queryspace can order accoring to query all by itself, i.e. 
//   * without an extra in-memory step introduced by scray-querying the
//   * results will be ordered if the queryspace can choose the main table
//   */
//  def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration]
//  
//  /**
//   * if this queryspace can group accoring to query all by itself, i.e. 
//   * without an extra in-memory step introduced by scray-querying
//   */
//  def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration]
//  
//  /**
//   * If this queryspace can handle the query using the materialized view provided.
//   * The weight (Int) is an indicator for the specificity of the view and reflects the 
//   * number of columns that match query arguments.
//   */
//  def queryCanUseMaterializedView(query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)]
//  
//  /**
//   * returns configuration of tables which are included in this query space
//   * Internal use! 
//   */
//  def getTables(version: Int): Set[TableConfiguration[_, _, _]]
//  
//  /**
//   * returns columns which can be included in this query space
//   * Internal use! 
//   */
//  def getColumns(version: Int): List[ColumnConfiguration]
//  
//  /**
//   * re-initialize this queryspace, possibly re-reading the configuration from somewhere
//   */
//  def reInitialize(oldversion: Int): QueryspaceConfiguration
//  
//  
//}