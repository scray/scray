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

class JDBCStatementsSpecs extends WordSpec with BeforeAndAfterAll with LazyLogging {

  var db: Database = null

  override def beforeAll() = {
    val h2Url = "jdbc:h2:mem:test;MODE=MySql;DATABASE_TO_UPPER=false"
    db = Database.forURL(url = h2Url, driver = "org.h2.Driver")
  }

  "JDBCStatementsSpecs " should {
    "register job in sync table " in {

      val syncApi = new SyncTableComponent(slick.jdbc.OracleProfile)
      val jobInfo = JDBCJobInfo("job1", 3, 2)

      db.run(syncApi.create).onComplete(_ match {
          case Success(lines) => ""
          case Failure(ex) =>  {
            logger.error(s"Unable to execute statement ${ex}")
            fail();
          };
       }
      )

      // 5 statements schould be generatd. 3 batch versions and 2 online versions
      assert(syncApi.registerJob(jobInfo).size === 5)

      // Register job
      syncApi.registerJob(jobInfo).map(db.run(_).onComplete(_ match {
          case Success(lines) => ""
          case Failure(ex) =>  {
            logger.error(s"Unable to execute statement ${ex}")
            fail();
          };
       }
      ))

      db.close()
    }
  }

}