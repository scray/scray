package scray.querying.sync

import java.util.{ Iterator => JIterator }
import scray.querying.sync.types.State.State
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
import scala.util.Try


abstract class OnlineBatchSync extends LazyLogging {

  /**
   * Generate and register tables for a new job.
   */
  def initJob[T <: ArbitrarylyTypedRows](job: JobInfo, dataTable: T): Try[Unit]
  
  def startNextBatchJob(job: JobInfo): Try[Unit]
  def startNextOnlineJob(job: JobInfo): Try[Unit]
  
  def getRunningBatchJobVersion(job: JobInfo): Option[Int]
  def getRunningOnlineJobVersion(job: JobInfo): Option[Int]
  
  def insertInBatchTable(jobName: JobInfo, nr: Int, data: RowWithValue): Try[Unit]
  def insertInOnlineTable(jobName: JobInfo, nr: Int, data: RowWithValue): Try[Unit]
  
  def completeBatchJob(job: JobInfo): Try[Unit]
  def completeOnlineJob(job: JobInfo): Try[Unit]
  
  def getOnlineJobState(job: JobInfo, version: Int): Option[State]
  def getBatchJobState(job: JobInfo, version: Int): Option[State]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, nr: Int, result: T): Option[List[RowWithValue]]
  def getBatchJobData[T <: RowWithValue](jobname: String, nr: Int, result: T): Option[List[RowWithValue]]
  
 
  
  
//  /**
//   * Check if tables exists and tables are locked
//   */
//  def initJobWorker(jobName: String, numberOfBatches: Int, dataTable: T)
//
  /**
   * Lock online table if it is used by another spark job.
   */
  def lockOnlineTable(job: JobInfo): Try[Unit]
   /**
   * Unlock online table to make it available for a new job.
   */
  def unlockOnlineTable(job: JobInfo): Try[Unit]
  //def isOnlineTableLocked(jobName: JobInfo): Boolean
  
   /**
   * Lock online table if it is used by another spark job.
   */
  def lockBatchTable(job: JobInfo): Try[Unit]

//   /**
//   * Unlock batch table to make it available for a new job.
//   */
//  def unlockBatchTable(jobName: String, nr: Int): Boolean
//  
//  def getHeadBatch(jobName: String): Option[Int]
//  

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

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)