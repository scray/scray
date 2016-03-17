package scray.querying.sync

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
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.sync.types._
import scray.querying.sync.types._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer

abstract class OnlineBatchSync extends LazyLogging {

  /**
   * Generate and register tables for a new job.
   */
  def createNewJob[T <: ArbitrarylyTypedRows](job: JobInfo, dataTable: T)
  
//  /**
//   * Check if tables exists and tables are locked
//   */
//  def initJobWorker(jobName: String, numberOfBatches: Int, dataTable: T)
//
  /**
   * Lock online table if it is used by another spark job.
   */
  def lockOnlineTable(job: JobInfo): Boolean
   /**
   * Unlock online table to make it available for a new job.
   */
  def unlockOnlineTable(job: JobInfo): Boolean
  //def isOnlineTableLocked(jobName: JobInfo): Boolean
  
   /**
   * Lock online table if it is used by another spark job.
   */
  def lockBatchTable(job: JobInfo): Boolean

//   /**
//   * Unlock batch table to make it available for a new job.
//   */
//  def unlockBatchTable(jobName: String, nr: Int): Boolean
//  
//  def getHeadBatch(jobName: String): Option[Int]
//  
  def insertInBatchTable(jobName: JobInfo, nr: Int, data: RowWithValue)
  def insertInOnlineTable(jobName: JobInfo, nr: Int, data: RowWithValue)
//  
//  /**
//   * Returns next job number of no job is currently running.
//   */
//  def getNextBatch(): Option[Int]
//  
//  //def getJobData[ColumnsT <: Columns[_]](jobName: String, nr: Int): ColumnsT
//  
//  /**
//   * Delete SyncTable and all datatables.
//   */
//  def purgeAllTables()
  
  // def getSyncTable: Table[SyncTableColumnsValues[List[_]]]
}


class JobInfo(
  val name: String,
  val numberOfBatcheVersions: Int = 3,
  val numberOfOnlineVersions: Int = 3
  ) {}

object JobInfo {
 def apply(name: String) = {
    new JobInfo(name)
  }
}