package scray.loader

import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.internal.MaterializedView
import scray.querying.queries.DomainQuery
import scray.querying.description.ColumnConfiguration
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.loader.configparser.ScrayQueryspaceConfiguration
import scray.loader.configparser.ScrayConfiguration
import scray.loader.configuration.ScrayStores
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import scala.collection.mutable.HashMap
import scray.querying.storeabstraction.StoreGenerators
import scray.querying.sync.types.DbSession
import scray.querying.description.ColumnConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.description.VersioningConfiguration
import com.twitter.storehaus.QueryableStore
import scray.querying.description.Row
import scray.querying.description.Column


/**
 * a generic query space that can be used to load tables from
 * various different databases.
 */
class ScrayLoaderQuerySpace(name: String, config: ScrayConfiguration, qsConfig: ScrayQueryspaceConfiguration,
    storeConfig: ScrayStores) 
    extends QueryspaceConfiguration(name) with LazyLogging {

  val generators = new HashMap[String, StoreGenerators]
  val extractors = new HashMap[TableIdentifier, StoreExtractor[_]]
  storeConfig.addSessionChangeListener { (name, session) => generators -= name }
  
  val version = qsConfig.version
  
  /**
   * if this queryspace can order accoring to query all by itself, i.e. 
   * without an extra in-memory step introduced by scray-querying the
   * results will be ordered if the queryspace can choose the main table
   */
  def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration] = ???
  
  /**
   * if this queryspace can group accoring to query all by itself, i.e. 
   * without an extra in-memory step introduced by scray-querying
   */
  def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration] = ???
  
  /**
   * If this queryspace can handle the query using the materialized view provided.
   * The weight (Int) is an indicator for the specificity of the view and reflects the 
   * number of columns that match query arguments.
   */
  def queryCanUseMaterializedView(query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)] = ???
  
  /**
   * return a generator for the given named dbms
   */
  private def getGenerator(dbmsId: String, session: DbSession[_, _, _]) = {
    generators.get(dbmsId).getOrElse {
      val generator = storeConfig.getStoreGenerator(dbmsId, session, name)
      generators += ((dbmsId, generator))
      generator
    }
  }

  /**
   * return configuration for a simple rowstore
   */
  private def getRowstoreConfiguration(id: TableIdentifier): Option[TableConfiguration[_, _, _]] = {
    // Extractor
    def extractTable[S <: QueryableStore[_, _]](storeconfigs: (S, 
        (Function1[_, Row], Option[String], Option[VersioningConfiguration[_, _, _]])), generator: StoreGenerators) = {
      // TODO: read latest version from SyncTable, if it is declared there, generate a VersioningConfig; otherwise leave it by None
      val extractor = generator.getExtractor(storeconfigs._1, Some(id.tableId), None)
      val tid = extractor.getTableIdentifier(storeconfigs._1, storeconfigs._2._2)
      extractors.+=((tid, extractor))
      extractor.getTableConfiguration(storeconfigs._2._1)
    } 
    // retrieve session...
    val session = storeConfig.getSessionForStore(id.dbSystem)
    // TODO: add session change listener to change store in case of session change
    // storeConfig.addSessionChangeListener(listener)
    session.flatMap { sess =>
      val generator = getGenerator(id.dbId, sess)
      val sStore = generator.createRowStore(id)
      sStore.map { storeconfigs =>
        extractTable(storeconfigs, generator)
      }
    }    
  }
  
  /**
   * returns configuration of tables which are included in this query space
   * Internal use! 
   */
  def getTables(version: Int): Set[TableConfiguration[_, _, _]] = {
    val generators = new HashMap[String, StoreGenerators]
    // TODO: read versioned tables from SyncTable and add to rowstores
    val rowstores = qsConfig.rowStores
    rowstores.map { tableConfigTxt =>
      getRowstoreConfiguration(tableConfigTxt)
    }.collect { 
      case Some(tableConfiguration) => tableConfiguration
    }.toSet
    // TODO: add more tables (for the ones in the queryspace config, e.g. indexes) 
  }
  
  /**
   * returns columns which can be included in this query space
   * Internal use! 
   */
  override def getColumns(version: Int): List[ColumnConfiguration] = {
    def getColumnConfig[S <: TableConfiguration[_, _, _]](table: S): List[ColumnConfiguration] = {
      def extractTableConfig[F <: QueryableStore[_, _]](column: Column, extractor: StoreExtractor[F]) = {
        // TODO: add indexing configuration (replace maps)
        val index = extractor.createManualIndexConfiguration(column, name, version, table.queryableStore.get().asInstanceOf[F], Map(), Map())
        // TODO: add splitter configuration
        extractor.getColumnConfiguration(table.asInstanceOf[F], column, this, index, Map())        
      }
      def throwError: Exception = {
        logger.error("Store must be registered before columns can be extracted!")
        new UnsupportedOperationException("Store must be registered before columns can be extracted!") 
      }
      table.allColumns.map { column =>
        // fetch extractor 
        extractors.get(column.table).getOrElse {
          throw throwError
        } match {
          case extractor: StoreExtractor[s] => extractTableConfig[s](column, extractor)
          case _ => throw throwError
        }
      }
    }
    val tables = getTables(version)
    tables.flatMap { table => getColumnConfig(table) }.toList 
  }
  
  /**
   * re-initialize this queryspace, possibly re-reading the configuration from somewhere
   */
  def reInitialize(oldversion: Int): QueryspaceConfiguration = ???
}