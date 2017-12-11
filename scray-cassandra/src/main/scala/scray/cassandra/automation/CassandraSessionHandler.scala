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
package scray.cassandra.automation

import java.util.concurrent.locks.ReentrantLock

import com.datastax.driver.core.{Cluster, Metadata, Session}

import scala.collection.mutable.HashMap

/**
 * handles sessions for Cassandra clusters based on dbid and keyspace 
 */
class CassandraSessionHandler {
  
  val sessionLock = new ReentrantLock // lock for sessions
  val sessions = new HashMap[(String, String), Session]
  
  /**
   * create or get a session object for the keyspace
   */
  def getSession(sessionid: (String, String), cluster: Cluster): Session = {
    sessionLock.lock
    try {
      sessions.get(sessionid) match {
        case None =>
          val session = cluster.connect(Metadata.quote(sessionid._2))
          sessions.put(sessionid, session)
          session
        case Some(session) => session
      }
    } finally {
      sessionLock.unlock()
    }
  }

  
}