package scray.cassandra.configuration

import com.twitter.util.JavaTimer
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import scray.querying.Registry
import scray.cassandra.extractors.CassandraExtractor
import scala.collection.mutable.HashMap
import scray.querying.description.TableIdentifier
import scray.querying.description.TableConfiguration
import scray.querying.description.ColumnConfiguration

object CassandraConfigurationUpdater {
  
  lazy val updater = new JavaTimer(true).schedule(Duration(15, TimeUnit.MINUTES)) {
    Registry.getQuerySpaceNames().foreach { space =>
      val spaceVersion = Registry.getLatestVersion(space)
      val aggVersionedTables = updateVersionedAggregationTables(space, spaceVersion)
      val luceneTables = updateLuceneIndexes(space, spaceVersion, aggVersionedTables)
      Registry.updateQuerySpace(space, luceneTables)
    }
  }
  
  private val previousLuceneIndexes = new HashMap[TableIdentifier, String]
  private val previousVersions = new HashMap[TableIdentifier, String]
  
  /**
   * Since in this module we are only interested in Cassandra tables, we want only those...
   */
  private def filterOutOtherThanCassandraTables(space: String, spaceVersion: Option[Int]): Map[TableIdentifier, TableConfiguration[_, _, _]] = {
    spaceVersion.map { version => 
      Registry.getQuerySpaceTables(space, version).filter(_._1.dbSystem == CassandraExtractor.DB_ID)
    }.getOrElse(Map())
  }
  
  /**
   * 
   */
  def updateVersionedAggregationTables(space: String, spaceVersion: Option[Int], 
      updatedTables: Set[TableConfiguration[_, _, _]] = Set()): Set[TableConfiguration[_, _, _]] = {
    // internal method to allow Scala's type inference to work correctly
    @inline def tableUpgradeVersion[Q, K, V](table: TableConfiguration[Q, K, V]): TableConfiguration[Q, K, V] = {
      table.copy(versioned = None)
    }
    val tables = filterOutOtherThanCassandraTables(space, spaceVersion)
    // we only need to update versioned tables
    val versionedTables = tables.filter(table => table._2.versioned.isDefined).map(_._2).toSet ++ updatedTables
    // from those we need, we check if the version we have
    versionedTables.map(tableUpgradeVersion(_))
  }

  /**
   * Updating run, in which we query Cassandra for Lucene index changes.
   * This is a rather simple method, which of course only applies to Cassandra tables with Lucene indexes.
   * here we do not care about changed versions during updates
   */
  def updateLuceneIndexes(space: String, spaceVersion: Option[Int], 
      updatedTables: Set[TableConfiguration[_, _, _]] = Set()): Set[(TableIdentifier, TableConfiguration[_, _, _], List[ColumnConfiguration])] = {
    val tables = filterOutOtherThanCassandraTables(space, spaceVersion)
    // walk over all the tables and check that there is no new Lucene information available
    
    Set()
  }
  
}