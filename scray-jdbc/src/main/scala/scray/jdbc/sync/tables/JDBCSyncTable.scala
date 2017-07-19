package scray.jdbc.sync.tables

import slick.jdbc.JdbcProfile
import scala.concurrent.ExecutionContext.Implicits.global
import scray.querying.sync.JobInfo
import java.sql.PreparedStatement
import java.sql.ResultSet
import scray.querying.sync.State

trait DriverComponent {
  val driver: JdbcProfile
}


/** UserComponent provides database definitions for User objects */
class SyncTableComponent(val driver: JdbcProfile) { 
  import driver.api._

  class SyncTable(tag: Tag) extends Table[(String, Int, Int, String, String, String, Long, Long, Boolean, String, String, Long)](tag, "TBDQSYNCTABLE") {
    def jobname = column[String]("CJOBNAME")
    def slot = column[Int]("CSLOT")
    def versions = column[Int]("CVERSIONS")
    def dbSystem = column[String]("CDBSYSTEM")
    def dbId = column[String]("CDBID")
    def tableID = column[String]("CTABLEID")
    def batchStartTime = column[Long]("CBATCHSTARTTIME")
    def batchEndTime = column[Long]("CBATCHENDTIME")
    def online = column[Boolean]("CONLINE")
    def state = column[String]("CSTATE")
    def mergeMode = column[String]("CMERGEMODE")
    def firstElementTime = column[Long]("CFIRSTELEMENTTIME")

    def * = (jobname, slot, versions, dbSystem, dbId, tableID, batchStartTime, batchEndTime, online, state, mergeMode, firstElementTime)
  }
  
    val table = TableQuery[SyncTable]
    
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
      max
    }

   
}