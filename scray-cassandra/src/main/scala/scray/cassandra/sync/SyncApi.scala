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

package scray.cassandra.sync

import scray.querying.sync.SyncApi
import scray.querying.description.TableIdentifier
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.JobInfo
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.DbSession
import scray.querying.sync.OnlineBatchSyncWithTableIdentifier

object CassandraSyncApi extends SyncApi {
  def init[Statement, InsertIn, Result]                                        (host: String, job: JobInfo[Statement, InsertIn, Result], ti: TableIdentifier): Option[OnlineBatchSyncWithTableIdentifier[Statement, InsertIn, Result]] = ???
  def init[Statement, InsertIn, Result, DataTableT <: ArbitrarylyTypedRows]    (host: String, job: JobInfo[Statement, InsertIn, Result], dataTable: DataTableT): Option[OnlineBatchSync[Statement, InsertIn, Result]] = ???
  def init[Statement, InsertIn, Result, T]                                     (dbSession: DbSession[Statement, InsertIn, Result, T], ti: TableIdentifier): Option[OnlineBatchSyncWithTableIdentifier[Statement, InsertIn, Result]] = ???
  def init[Statement, InsertIn, Result, T, DataTableT <: ArbitrarylyTypedRows] (dbSession: DbSession[Statement, InsertIn, Result, T], dataTable: DataTableT): Option[OnlineBatchSyncWithTableIdentifier[Statement, InsertIn, Result]] = ???
}