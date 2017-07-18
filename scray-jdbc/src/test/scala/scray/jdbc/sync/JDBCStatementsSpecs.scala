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
import scray.jdbc.sync.JDBCImplementation._



class JDBCStatementsSpecs extends WordSpec with LazyLogging  {
  "JDBCStatementsSpecs " should {
    "build create statement for sync table " in {
      
      val syncTable = SyncTable("db1", "table1")
      
      val statement = JDBCStatements.createTableStatement(syncTable)
      
      println(statement)

    }
  }
  
}