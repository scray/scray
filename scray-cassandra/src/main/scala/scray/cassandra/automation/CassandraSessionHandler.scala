package scray.cassandra.automation

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.{StoreCluster, StoreSession}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.HashMap
import com.datastax.driver.core.Cluster

/**
 * handles sessions for Cassandra clusters based on dbid and keyspace 
 */
class CassandraSessionHandler {
  
  val sessionLock = new ReentrantLock // lock for sessions
  val sessions = new HashMap[(String, String), StoreSession]
  
  /**
   * create or get a session object for the keyspace
   */
  def getSession(sessionid: (String, String), cluster: Cluster): StoreSession = {
    sessionLock.lock
    try {
      sessions.get(sessionid) match {
        case None =>
          val session = new StoreSession(sessionid._2, None, Some(cluster))
          sessions.put(sessionid, session)
          session
        case Some(session) => session
      }
    } finally {
      sessionLock.unlock()
    }
  }

  
}