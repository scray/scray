package scray.jdbc.automation

import scray.querying.storeabstraction.StoreGenerators
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.description.VersioningConfiguration
import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import com.twitter.util.FuturePool
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.Row
import scray.jdbc.extractors.ScraySQLDialect
import java.sql.Connection
import com.zaxxer.hikari.HikariDataSource
import scray.jdbc.extractors.JDBCExtractors
import scray.jdbc.extractors.JDBCHiveExtractors
import scray.querying.description.ColumnConfiguration
import scray.jdbc.extractors.DomainToSQLQueryMapping
import scray.jdbc.JDBCQueryableSource
import scray.jdbc.rows.JDBCRowMapper
import scray.jdbc.JDBCHiveQueryableSource
import scray.jdbc.extractors.DomainToHiveSQLQueryMapping
import scray.querying.description.Column
import scray.querying.source.Splitter
import scray.querying.description.ManuallyIndexConfiguration
import com.typesafe.scalalogging.LazyLogging

/**
 * store generator for JDBC
 */
class JDBCStoreGenerators(hikari: HikariDataSource, metadataConnection: Connection, sqlDialect: ScraySQLDialect, futurePool: FuturePool) 
  extends StoreGenerators with LazyLogging {
  
  var sqlDialectName: String = ""
  
  /**
   * creates a row store, i.e. a store that has a primary key column and maybe a bunch of other columns
   */
	def createRowStore[Q <: DomainQuery](table: TableIdentifier): Option[(QueryableStoreSource[Q], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]]))] = {
			
					logger.error("d" + sqlDialect.getName)
					sqlDialectName = sqlDialect.getName
					
					if(sqlDialect.getName.startsWith("HIVE"))  {
					  val extractor = new JDBCHiveExtractors(table, hikari, metadataConnection, sqlDialect, futurePool)
						val rowMapper = new JDBCRowMapper(table)
								val source = new JDBCHiveQueryableSource(table, 
										extractor.getRowKeyColumns,
										extractor.getClusteringKeyColumns,
										extractor.getColumns,
										extractor.getColumnConfigurations(
										    null, 
										    table.dbId, 
										    table.tableId, 
										    null, 
										    Map.empty[String, ManuallyIndexConfiguration[_ <: DomainQuery, _ <: DomainQuery, _, _, _ <: DomainQuery]],
										    Map.empty[Column, Splitter[_]]
										    ),
										hikari,
										new DomainToHiveSQLQueryMapping[Q, JDBCHiveQueryableSource[Q]](),
										futurePool,
										rowMapper,
										sqlDialect    
										)
								Some((source, (rowMapper, None, None))) }
					else {
					  val extractor = new JDBCExtractors(table, hikari, metadataConnection, sqlDialect, futurePool)
						val rowMapper = new JDBCRowMapper(table)
								val source = new JDBCQueryableSource(table, 
										extractor.getRowKeyColumns,
										extractor.getClusteringKeyColumns,
										extractor.getColumns,
										extractor.getColumnConfigurations(null, table.dbId, table.tableId, null, Map(), Map()),
										hikari,
										new DomainToSQLQueryMapping[Q, JDBCQueryableSource[Q]](),
										futurePool,
										rowMapper,
										sqlDialect    
										)
								Some((source, (rowMapper, None, None))) }
	}

  /**
   * gets the extractor, that helps to evaluate meta-data for this type of dbms
   */
  def getExtractor[Q <: DomainQuery, S <: QueryableStoreSource[Q]](
      store: S, tableName: Option[String], versions: Option[VersioningConfiguration[_, _]], 
      dbSystem: Option[String], futurePool: FuturePool): StoreExtractor[S] = {
    
    if(sqlDialectName.startsWith("HIVE")) 
      new JDBCHiveExtractors[Q, JDBCHiveQueryableSource[Q]](store.asInstanceOf[JDBCHiveQueryableSource[Q]].ti, hikari, 
        metadataConnection, sqlDialect, futurePool).asInstanceOf[StoreExtractor[S]]
    else    
    new JDBCExtractors[Q, JDBCQueryableSource[Q]](store.asInstanceOf[JDBCQueryableSource[Q]].ti, hikari, 
        metadataConnection, sqlDialect, futurePool).asInstanceOf[StoreExtractor[S]]
  }
 
}
