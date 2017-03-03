package scray.jdbc.automation

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.RowColumn
import scray.jdbc.JDBCQueryableSource
import com.twitter.util.Await
import java.sql.Connection
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner
import scray.querying.description.TableIdentifier
import com.twitter.util.FuturePool
import scray.jdbc.rows.JDBCRow
import scray.jdbc.extractors.DomainToSQLQueryMapping
import scray.querying.description.ColumnConfiguration
import scray.querying.description.Column
import scray.querying.queries.DomainQuery
import java.util.concurrent.Executors
import org.scalatest.mock.MockitoSugar
import com.twitter.util.Future
import com.twitter.concurrent.Spool
import scray.querying.description.Row

@RunWith(classOf[JUnitRunner])
class JDBCQueryableSourceTest extends WordSpec {
  
  "JDBCQueryableSource " should {
    
    " create iterator from collection " in {
      val row =  SimpleRow(ArrayBuffer.empty[RowColumn[_]])
      val data = row :: row :: row :: row :: Nil
      
      val dataSpool = JDBCQueryableSource.toRowSpool(data.iterator)
      val countRows = dataSpool.get.foldLeft(0)((acc: Int,row: Row) => (acc + 1))
      
      assert(4 === Await.result(countRows))
    }
    " query database " in {
      
      val ti = TableIdentifier("oracle", "mytestspace", "mycf")     
      val mapper = (row: JDBCRow) => SimpleRow(ArrayBuffer.empty[RowColumn[_]])
      val connection = MockitoSugar.mock[Connection]

      val jdbcSource =  new JDBCQueryableSource(
          ti, 
          Set.empty[Column], 
          Set.empty[Column], 
          Set.empty[Column], 
          Set.empty[ColumnConfiguration], 
          connection, 
          new DomainToSQLQueryMapping[DomainQuery, JDBCQueryableSource[DomainQuery]], 
          FuturePool(Executors.newCachedThreadPool()), 
          mapper)
    }

  }
}