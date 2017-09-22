package scray.jdbc.sync.tables

import slick.jdbc.JdbcProfile
import scala.concurrent.ExecutionContext.Implicits.global
import scray.querying.sync.JobInfo
import java.sql.PreparedStatement
import java.sql.ResultSet
import scray.querying.sync.State
import slick.sql.FixedSqlAction
import java.util.ArrayList
import scala.collection.mutable.MutableList
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer


trait DriverComponent {
  val driver: JdbcProfile
}

/** 
 *  SyncTableComponent provides database definitions for SyncTable table
 *  and some statements to manipulate data.
 *  
 *  No semantic checks are made to protect from manipulating wrong data.
 *  
 *   
 */
class SyncTableComponent(val driver: JdbcProfile, val dbSystemId: String = "BISDBADMIN", val tablename: String = "TBDQSYNCTABLE") {
  import driver.api._

  case class JDBCSyncTable(
    jobname: String,
    slot: Int,
    versions: Int,
    dbId: String,
    tableID: String,
    batchStartTime: Option[Long] = None,
    batchEndTime: Option[Long] = None,
    online: Boolean,
    state: String,
    mergeMode: Option[String] = None,
    firstElementTime: Option[Long] = None)
    
  class SyncTableT(tag: Tag) extends Table[JDBCSyncTable](tag, tablename) {
    def jobname = column[String]("CJOBNAME")
    def slot = column[Int]("CSLOT")
    def versions = column[Int]("CVERSIONS")
    def dbId = column[String]("CDBID")
    def tableID = column[String]("CTABLEID")
    def batchStartTime = column[Option[Long]]("CBATCHSTARTTIME")
    def batchEndTime = column[Option[Long]]("CBATCHENDTIME")
    def online = column[Boolean]("CONLINE")
    def state = column[String]("CSTATE")
    def mergeMode = column[Option[String]]("CMERGEMODE")
    def firstElementTime = column[Option[Long]]("CFIRSTELEMENTTIME")

    def * = (jobname, slot, versions, dbId, tableID, batchStartTime, batchEndTime, online, state, mergeMode, firstElementTime) <> (JDBCSyncTable.tupled, JDBCSyncTable.unapply)
    def pk = primaryKey("pk_a", (jobname, online, slot))
  }

  val table = TableQuery[SyncTableT]

  /**
   * Create database schema
   */
  def create = table.schema.create


  /**
   * Register job in database.
   */
  def registerJobStatement(jobInfo: JobInfo[_, _, _]) = {

    // Statements to set start values in sync table
    def createSyncTableBatchEntries(slotIn: Int) = {
      table.insertOrUpdate(
        JDBCSyncTable(
          jobname = jobInfo.name,
          slot = slotIn,
          versions = jobInfo.numberOfBatchSlots,
          dbId = dbSystemId,
          tableID = getBatchJobName(jobInfo.name, slotIn),
          online = false,
          state = State.NEW.toString()))
    }

    def createSyncTableOnlineEntries(slotIn: Int) = {
      table.insertOrUpdate(
        JDBCSyncTable(
          jobname = jobInfo.name,
          slot = slotIn,
          versions = jobInfo.numberOfOnlineSlots,
          dbId = dbSystemId,
          tableID = getOnlineJobName(jobInfo.name, slotIn),
          online = true,
          state = State.NEW.toString()))
    }

    val statements: ListBuffer[FixedSqlAction[Int, NoStream, Effect.Write]] = ListBuffer.empty[FixedSqlAction[Int, NoStream, Effect.Write]]

    // Create sync table statements with start values for a given job
    0 to jobInfo.numberOfBatchSlots - 1 foreach { slot =>
      statements += createSyncTableBatchEntries(slot)
    }
    
    0 to jobInfo.numberOfOnlineSlots - 1 foreach { slot =>
      statements += createSyncTableOnlineEntries(slot)
    }

    statements
  }
  
  
  def startJobStatement(jobInfo: JobInfo[_, _, _], slot: Int, online: Boolean) = {
     val row = table.
      filter(_.jobname === jobInfo.name).
      filter(_.online === online).
      filter(_.slot === slot).
      map(_.state)
      
      row.update(State.RUNNING.toString())
  }
  
  def completeJobStatement(jobInfo: JobInfo[_, _, _], slot: Int, online: Boolean, completionTime: Long) = {
     val row = table.
      filter(_.jobname === jobInfo.name).
      filter(_.online === online).
      filter(_.slot === slot).
      map(row => (row.state, row.batchEndTime))
      
      row.update((State.COMPLETED.toString(), Some(completionTime)))
  }
  
  def getRunningJobStatement(jobInfo: JobInfo[_, _, _], online: Boolean) = {
    table.
      filter(_.jobname === jobInfo.name).
      filter(_.online === online).
      filter(_.state === State.RUNNING.toString()).
      result
  }
  
    /**
   * Return batch job row with the newest time stamp
   */
  def getLatestCompletedJobStatement(jobInfo: JobInfo[_, _, _], online: Boolean) = {
    table.
      filter(_.jobname === jobInfo.name).
      filter(_.online === online).
      filter(_.state === State.COMPLETED.toString()).
      sortBy(_.batchEndTime.desc).
      take(1).
      result
  }

  
  private def getBatchJobName(jobname: String, nr: Int): String = { jobname + "_batch" + nr }
  private def getOnlineJobName(jobname: String, nr: Int): String = { jobname + "_online" + nr }

}