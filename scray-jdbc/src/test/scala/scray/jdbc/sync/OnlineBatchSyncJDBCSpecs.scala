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
import org.scalatest.BeforeAndAfterAll
import scala.util.Success
import scala.util.Failure
import scala.collection.mutable.HashMap

class OnlineBatchSyncJDBCSpecs extends WordSpec with BeforeAndAfterAll with LazyLogging {

  override def beforeAll() = {
    val h2Url = "jdbc:h2:mem:test;MODE=MySql;DATABASE_TO_UPPER=true;DB_CLOSE_DELAY=-1"
    Database.forURL(url = h2Url, driver = "org.h2.Driver")
  }

  "OnlineBatchSyncJDBCSpecs " should {
        " set and get start points of streaming nodes " in {
    
          val jobInfo = new JDBCJobInfo("job1")
          JDBCDbSession.getNewJDBCDbSession("jdbc:h2:mem:test;MODE=MySql", "scray", "scray")
            .map(new OnlineBatchSyncJDBC(_))
            .map { syncApi => {
                syncApi.initJobIfNotExists(jobInfo)
                syncApi.startNextOnlineJob(jobInfo)
                
                syncApi.setOnlineStartPoint(jobInfo, 1, "data_1") 
                syncApi.setOnlineStartPoint(jobInfo, 2, "data_2") 
                
                syncApi.completeOnlineJob(jobInfo)
                
                val startPoints = syncApi.getOnlineStartPointAsString(jobInfo, 0)
                
                assert(startPoints.head.startPoint      == "data_2")
                assert(startPoints.tail.head.startPoint == "data_1")                
              } 
            }
          
          match {
              case Success(lines) => 
              case Failure(ex)    => println(s"Problem rendering URL content: ${ex.printStackTrace()}")
            }
        }
  }

}