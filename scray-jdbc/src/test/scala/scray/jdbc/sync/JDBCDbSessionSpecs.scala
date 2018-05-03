package scray.jdbc.sync

import scala.util.Failure
import scala.util.Success

import org.scalatest.Tag
import org.scalatest.WordSpec

import junit.framework.Assert
import scray.jdbc.extractors.ScraySQLDialectFactory
import scray.jdbc.sync.tables.SyncTableComponent

object RequiresMySQLServer extends Tag("scray.jdbc.tags.RequiresMySQLServer")

class JDBCDbSessionSpecs  extends WordSpec {
  
  /*
   * Create database first
   * 
   * CREATE DATABASE IF NOT EXISTS scray;
	 * GRANT ALL ON scray.* TO 'scray'@'%' IDENTIFIED BY 'scray';
   */
  
  "JDBCStatementsSpecs " should {
    " create scray sync table " taggedAs(RequiresMySQLServer) in {
      
      // Establish connection
      val session = new JDBCDbSession("jdbc:mariadb://127.0.0.1:3306/scray",ScraySQLDialectFactory.getDialect("mariadb"), "scray", "scray")

      val syncApi = new SyncTableComponent(slick.jdbc.MySQLProfile)
     
      syncApi.create.statements.map { x => session.execute(x) match {
        case Success(row) => Assert.assertTrue(true)
        case Failure(ex) => Assert.fail(s"Exception: ${ex.getMessage}")
        }
      }       
    }
  }
  
}