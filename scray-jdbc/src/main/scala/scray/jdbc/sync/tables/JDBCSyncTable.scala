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

/** UserComponent provides database definitions for User objects */
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

  def getLatestBatch(jobInfo: JobInfo[_, _, _]) = {
    table.
      filter(_.jobname === jobInfo.name).
      filter(_.online === false).
      filter(_.state === State.COMPLETED.toString()).
      map(_.batchEndTime).
      max.result
  }

  def registerJob(jobInfo: JobInfo[_, _, _]) = {

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

  private def getBatchJobName(jobname: String, nr: Int): String = { jobname + "_batch" + nr }
  private def getOnlineJobName(jobname: String, nr: Int): String = { jobname + "_online" + nr }

}