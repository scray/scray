package scray.jdbc.extractors

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.RowColumn
import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import java.util.concurrent.Executors
import org.scalatest.mock.MockitoSugar
import java.sql.Connection
import scray.querying.description.SimpleRow
import scray.querying.description.ColumnConfiguration
import com.twitter.util.FuturePool
import scray.jdbc.JDBCQueryableSource
import scray.jdbc.rows.JDBCRow
import scray.querying.description.Column
import scala.collection.mutable.ArrayBuffer
import com.twitter.util.Await
import scray.querying.description.Row
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet

// @RunWith(classOf[JUnitRunner])
class JDBCQueryableSourceSpecs extends WordSpec {
  
  "JDBCQueryableSource " should {
    
    " create iterator from collection " in {
      val row =  SimpleRow(ArrayBuffer.empty[RowColumn[_]])
      val data = row :: row :: row :: row :: Nil
      
      val dataSpool = JDBCQueryableSource.toRowSpool(data.iterator)
      val countRows = Await.result(dataSpool).foldLeft(0)((acc: Int,row: Row) => (acc + 1))
      
      assert(4 === Await.result(countRows))
    }
    " query database " in {
      
      val ti = TableIdentifier("oracle", "mytestspace", "mycf")     
      val mapper = (row: ResultSet) => SimpleRow(ArrayBuffer.empty[RowColumn[_]])
      val connection = MockitoSugar.mock[HikariDataSource]

      val jdbcSource =  new JDBCQueryableSource(
          ti, 
          Set.empty[Column], 
          Set.empty[Column], 
          Set.empty[Column], 
          Set.empty[ColumnConfiguration], 
          connection, 
          new DomainToSQLQueryMapping[DomainQuery, JDBCQueryableSource[DomainQuery]], 
          FuturePool(Executors.newCachedThreadPool()), 
          mapper,
          ScraySQLDialectFactory.getDialect("ORACLE"))
    }

  }
}