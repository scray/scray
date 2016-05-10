package scray.querying.sync.cassandra

import com.websudos.phantom.CassandraPrimitive
import scray.querying.sync.types.DBColumnImplementation
import java.util.{ Iterator => JIterator }
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import scray.querying.sync.types._
import scray.querying.sync.types._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scray.querying.sync.types.SyncTableBasicClasses.SyncTableRowEmpty
import scala.collection.mutable.ListBuffer
import com.datastax.driver.core.BatchStatement
import scray.querying.sync.types.State.State
import com.datastax.driver.core.querybuilder.Update.Where
import com.datastax.driver.core.querybuilder.Update.Conditions
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConversions._
import scray.querying.description.TableIdentifier
import scala.collection.mutable.HashSet
import com.datastax.driver.core.querybuilder.Select
import scray.querying.sync.types.VoidTable
import scray.querying.sync.types.Table
import scray.querying.sync.types.SyncTable
import scray.querying.sync.types.State
import scray.querying.sync.types.RowWithValue
import scray.querying.sync.types.JobLockTable
import scray.querying.sync.types.DbSession
import scray.querying.sync.types.ColumnWithValue
import scray.querying.sync.types.AbstractRow
import scray.common.serialization.BatchID
import scray.querying.sync.types.SyncTableBasicClasses.JobLockTable
import scray.querying.sync.JobInfo
import java.util.concurrent.TimeUnit
import scala.None
import com.typesafe.scalalogging.slf4j.LazyLogging

class CassandraJobInfo(
    name: String,
    numberOfBatchSlots: Int = 3,
    numberOfOnlineSlots: Int = 2,
    lockTimeOut: Int = 500) extends JobInfo[Statement, Insert, ResultSet](name, numberOfBatchSlots, numberOfOnlineSlots) with LazyLogging {

  val statementGenerator = new CassandraStatementGenerator

  def getLock(dbSession: DbSession[Statement, Insert, ResultSet]): LockApi[Statement, Insert, ResultSet] = {
     this.lock = this.lock.orElse {
      val table = JobLockTable("SILIDX", "JobSync")

      dbSession.execute(statementGenerator.createKeyspaceCreationString(table).get).
        recover {
          case e => { logger.error(s"Synctable is unable to create keyspace ${table.keySpace} Message: ${e.getMessage}"); throw e }
        }.flatMap { _ =>
          dbSession.execute(statementGenerator.createSingleTableString(table).get)
        }

      // Register new job in lock table
      dbSession.execute(QueryBuilder.insertInto(table.keySpace, table.tableName)
        .value(table.columns.jobname.name, this.name)
        .value(table.columns.locked.name, false))

      Some(new CassandraSyncTableLock(this, JobLockTable("SILIDX", "JobSync"), dbSession, lockTimeOut))
    }

    this.lock.get
  }

  def getLock(dbHostname: String): LockApi[Statement, Insert, ResultSet] = {
    this.getLock(new CassandraDbSession(Cluster.builder().addContactPoint(dbHostname).build().connect()))
  }
}

object CassandraJobInfo {
  def apply(name: String) = {
    new CassandraJobInfo(name)
  }
  
  def apply(name: String, batchID: BatchID, numberOfBatchVersions: Int, numberOfOnlineVersions: Int) = {
    new CassandraJobInfo(name, numberOfBatchVersions, numberOfOnlineVersions)
  }
}
