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
package scray.jdbc.sync

import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging
import slick.jdbc.H2Profile.api._
import scray.querying.sync.Column
import scray.querying.sync.SyncTableBasicClasses
import scray.querying.sync.Table
import scray.querying.sync.SyncTable
import scray.jdbc.sync.tables.DriverComponent
import slick.jdbc.JdbcProfile
import slick.jdbc.OracleProfile
import com.zaxxer.hikari.HikariDataSource
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import com.zaxxer.hikari.HikariConfig
import scray.jdbc.sync.tables.SyncTableComponent
import slick.driver.H2Driver.api._
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import scray.cassandra.sync.JDBCJobInfo
import scala.concurrent.ExecutionContext.Implicits.global
import com.esotericsoftware.minlog.Log
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scray.querying.sync.State

class JDBCStatementsSpecs extends WordSpec with BeforeAndAfterAll with LazyLogging {

  var db: Database = null

  override def beforeAll() = {
    val h2Url = "jdbc:h2:mem:test;MODE=MySql;DATABASE_TO_UPPER=true;DB_CLOSE_DELAY=-1"
    db = Database.forURL(url = h2Url, driver = "org.h2.Driver")
  }

  "JDBCStatementsSpecs " should {
    "register job in sync table " in {
      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)

     val createTableResult = db.run(syncApi.create);
      
      // Create table
      createTableResult.onComplete(_ match {
        case Success(lines) => ""
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail()          
        };
      })

      try {
        Await.result(createTableResult, Duration("3 second"))
      } catch {
        case e: Exception => println(e) // FIXME fail in case of a exception
      }
      // 5 statements schould be generatd. 3 batch versions and 2 online versions
      assert(syncApi.registerJobStatement(jobInfo).size === 5)

      // Register job
      syncApi.registerJobStatement(jobInfo).map(db.run(_).onComplete(_ match {
        case Success(lines) => ""
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
      }))
      
println("BB")
    }
    " start batch job " in {
      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)
println("CC")
      // Mark batch job on slot 0 as running
      db.run(syncApi.startJobStatement(jobInfo, 0, false)).onComplete(_ match {
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
        case Success(x) =>
      })

      // Check if job1_batch0 is marked as running
      val tableIdRows = Await.result(db.run(syncApi.getRunningJobStatement(jobInfo, false)), Duration("1 second"))

      assert(tableIdRows.size == 1) // Only one job should be marked as running
      assert(tableIdRows.head.tableID == "job1_batch0")

    }
    " start online job " in {
      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)

      // Mark batch job on slot 0 as running
      db.run(syncApi.startJobStatement(jobInfo, 0, true)).onComplete(_ match {
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
        case Success(x) =>
      })

      // Check if job1_online0 is marked as running
      val tableIdRows = Await.result(db.run(syncApi.getRunningJobStatement(jobInfo, true)), Duration("1 second"))

      assert(tableIdRows.size == 1) // Only one job should be marked as running
      assert(tableIdRows.head.tableID == "job1_online0")
      
    }
    " complete batch job " in {
      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)

      // Mark batch job on slot 0 as completed
      db.run(syncApi.completeJobStatement(jobInfo, 0, false, 1500649303L)).onComplete(_ match {
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
        case Success(x) =>
      })

      // Check if job1_batch0 is marked as completed
      val tableIdRows = Await.result(db.run(syncApi.getLatestCompletedJobStatement(jobInfo, false)), Duration("1 second"))

      assert(tableIdRows.size == 1) // Only one latest completed job should exists
      assert(tableIdRows.head.state == State.COMPLETED.toString()) 
    }
    " complete online job " in {
      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)

      // Mark batch job on slot 0 as completed
      db.run(syncApi.completeJobStatement(jobInfo, 0, true, 1500649303L)).onComplete(_ match {
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
        case Success(x) =>
      })

      // Check if job1_batch0 is marked as completed
      val tableIdRows = Await.result(db.run(syncApi.getLatestCompletedJobStatement(jobInfo, true)), Duration("1 second"))

      assert(tableIdRows.size == 1) // Only one latest completed job should exists
      assert(tableIdRows.head.state == State.COMPLETED.toString())

    }
    " return latest completed batch job " in {
      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)
      
      val BATCH_END_TIME = 1500649399L

      // Mark batch job on slot 0 as completed
      db.run(syncApi.completeJobStatement(jobInfo, 0, false, BATCH_END_TIME)).onComplete(_ match {
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
        case Success(x) =>
      })

      // Check if job1_batch0 is marked as completed
      val tableIdRows = Await.result(db.run(syncApi.getLatestCompletedJobStatement(jobInfo, false)), Duration("1 second"))

      assert(tableIdRows.head.state == State.COMPLETED.toString()) 
      assert(tableIdRows.head.batchEndTime == Some(BATCH_END_TIME))  // Check if latest completed job is provided
    }
  
  }

}