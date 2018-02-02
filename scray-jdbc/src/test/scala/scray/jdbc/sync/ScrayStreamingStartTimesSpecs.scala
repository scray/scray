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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging
import slick.driver.H2Driver.api._
import slick.jdbc.H2Profile.api._
import scray.jdbc.sync.tables.ScrayStreamingStartTimesIO
import scala.util.Failure
import scala.util.Success
import slick.jdbc.H2Profile.api._
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
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ScrayStreamingStartTimesSpecs extends WordSpec with BeforeAndAfterAll with LazyLogging {

  var db: Database = null

  override def beforeAll() = {
    val h2Url = "jdbc:h2:mem:test;MODE=MySql;DATABASE_TO_UPPER=true;DB_CLOSE_DELAY=-1"
    db = Database.forURL(url = h2Url, driver = "org.h2.Driver")
  }

  "ScrayStreamingStartTimes " should {
    "create start time table " in {
      val table = new ScrayStreamingStartTimesIO(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)
      
      // Create table
      db.run(table.create).onComplete(_ match {
        case Success(lines) => 
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
      })
      
      // Check if table exits
      db.run(table.tableExists).onComplete(_ match {
        case Success(lines) => 
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
      })
    }
    " add some data to start time table " in {
      val table = new ScrayStreamingStartTimesIO(slick.jdbc.MySQLProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)
      
      // Create table
      db.run(table.setStartTime(jobInfo, 0, "data_1", 123L)).onComplete(_ match {
        case Success(lines) => 
        case Failure(ex) => {
          logger.error(s"Unable to execute statement ${ex}")
          fail();
        };
      })
      
      // Check if data exists in database
      val startTimeValues =  Await.result(db.run(table.getSartTimes(jobInfo, 0)), Duration("1 second"))
      if(startTimeValues.size == 1) {
        assert(startTimeValues.head.dataId === "data_1")
      } else {
        fail()
      }
    }
  }

}