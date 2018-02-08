// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
import slick.sql.FixedSqlStreamingAction
import scray.querying.sync.types.BatchMetadata

case class ScrayStreamingStartPoints[T](
  jobname: String,
  slot: Int,
  timestamp: Long,
  startPoint: T) // Identifies one data element


case class ScrayStreamingStartPointDb(
  jobname: String,
  slot: Int,
  timestamp: Long,
  startPoint: String // Identifies one data element
)  

class ScrayStreamingStartTimesIO(val driver: JdbcProfile, val dbSystemId: String = "SCRAY", val tablename: String = "TSCRAYSTREAMINGSTARTPOINTS") {
  import driver.api._

  class ScrayStreamingStartTimesTable(tag: Tag) extends Table[ScrayStreamingStartPointDb](tag, tablename) {
    def jobname = column[String]("CJOBNAME", O.Length(100))
    def slot = column[Int]("CSLOT")
    def timestamp = column[Long]("CTIMESTAMP")
    def startPoint = column[String]("CSTARTPOINT", O.Length(767))

    def * = (jobname, slot, timestamp, startPoint) <> (ScrayStreamingStartPointDb.tupled, ScrayStreamingStartPointDb.unapply)
    def pk = primaryKey("pk_scray_streaming_start_points", (jobname, slot, timestamp))
  }

  val table = TableQuery[ScrayStreamingStartTimesTable]

  /**
   * Create database schema
   */
  def create = table.schema.create

  /**
   * Check if table exists
   */
  def tableExists = {
    table.
      exists.
      result
  }
  
  def truncateSlot(jobInfo: JobInfo[_, _, _], slot: Int) = {
    table.
    filter(_.jobname === jobInfo.name).
    filter(_.slot === slot).
    delete
  }

  def setStartTime(jobInfo: JobInfo[_, _, _], slot: Int, timestamp: Long = System.currentTimeMillis(), startPoint: String) = {
    table.
      insertOrUpdate(
        ScrayStreamingStartPointDb(
          jobInfo.name,
          slot,
          timestamp,
          startPoint
        )
      )
  }
  
  def getSartTimes(jobInfo: JobInfo[_, _, _], slot: Int) = {
    table.
      filter(_.jobname === jobInfo.name).
      filter(_.slot === slot).
      result      
  }
}
