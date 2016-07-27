package scray.cassandra.automation

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.HashMap
import com.datastax.driver.core.{ Cluster, Session }

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
          val session = cluster.connect(sessionid._2)
          sessions.put(sessionid, session)
          session
        case Some(session) => session
      }
    } finally {
      sessionLock.unlock()
    }
  }

  
}