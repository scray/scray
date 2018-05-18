package scray.jdbc.osgi

import scray.jdbc.sync.JDBCDbSession
import scray.jdbc.sync.JDBCDbSessionImpl

class InstanceFactory  {

  def getJdbcSession(url: String, username: String, password: String): JDBCDbSession = {
    new SessionImpl(new JDBCDbSessionImpl(url, username, password))
  }
}