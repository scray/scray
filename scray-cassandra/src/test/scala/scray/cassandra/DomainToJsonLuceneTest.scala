package scray.cassandra

import java.util.logging.LogManager

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import scray.querying.description.Column
import scray.querying.description.ColumnOrdering
import scray.querying.description.TableIdentifier
import scala.math.Ordering.StringOrdering
import scala.math.Ordering.StringOrdering
import scray.querying.description.QueryRange
import scray.cassandra.extractors.DomainToJSONLuceneQueryMapper
import scray.querying.queries.DomainQuery
import scray.querying.description.internal.SingleValueDomain
import org.junit.Assert
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.Bound


  @RunWith(classOf[JUnitRunner])
class DomainToJsonLuceneTest extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {

  override def beforeAll() = {
    LogManager.getLogManager().reset();
  }


  "DomainToJsonLucene " should {
    " generate single order expression " in {
      
      // Define query withot a domaine
      val tableId = new TableIdentifier("cassandra", "cassandra", "table1")
      val column = new Column("column1", tableId)
      implicit val ordering = new Ordering[String] { def compare(a: String, b: String) = {0}}
      val columnOrdering =  new ColumnOrdering(column, false)

    val query = new DomainQuery(
      java.util.UUID.randomUUID,
      "queryspace1", 
      0,
      Set(column),
      tableId,
      List(),
      None,
      Some(columnOrdering),
      Some(QueryRange(None, None, None)))
    
      // Create lucene code.
      val queryString = DomainToJSONLuceneQueryMapper.getLuceneColumnsQueryMapping(query, List(), tableId)

      Assert.assertEquals(queryString.get, " lucene='{  sort : { fields : [ { field : \"column1\" , reverse : false } ] }  }' ")      
    }
    " generate order expression with domaine " in {
      
      // Define query with domaine
      val tableId = new TableIdentifier("cassandra", "cassandra", "table1")
      val column = new Column("column1", tableId)
      implicit val ordering = new Ordering[String] { def compare(a: String, b: String) = {0}}
      val columnOrdering =  new ColumnOrdering(column, false)
      val domaine = new SingleValueDomain(column, "42", false)

    val query = new DomainQuery(
      java.util.UUID.randomUUID,
      "queryspace1", 
      0,
      Set(column),
      tableId,
      List(domaine),
      None,
      Some(columnOrdering),
      Some(QueryRange(None, None, None)))
    
      // Create lucene code.
      val queryString = DomainToJSONLuceneQueryMapper.getLuceneColumnsQueryMapping(query, List(domaine), tableId)
      Assert.assertEquals(queryString.get, " lucene='{  { type : \"match\", field : \"column1\", value : \"42\" } ,  sort : { fields : [ { field : \"column1\" , reverse : false } ] }  }' ")      
    }
    " generate range query " in {
      // Define query with domaine
      val tableId = new TableIdentifier("cassandra", "cassandra", "table1")
      val column = new Column("column1", tableId)
      val domaine = new RangeValueDomain(column, Some(new Bound(true, 0)), Some(new Bound(true, 100)))

    val query = new DomainQuery(
      java.util.UUID.randomUUID,
      "queryspace1", 
      0,
      Set(column),
      tableId,
      List(domaine),
      None,
      None,
      Some(QueryRange(None, None, None)))
      
       // Create lucene code.
      val queryString = DomainToJSONLuceneQueryMapper.getLuceneColumnsQueryMapping(query, List(domaine), tableId)
      Assert.assertEquals(queryString.get, " lucene=\'{ filter : { type : \"range\", field : \"column1\",  lower: \"0\" , include_lower: \"true\" , upper: \"100\" , include_upper: \"true\"  }  }' ")
    }
    " generate range query with ordering " in {
       // Define query with domaine
      val tableId = new TableIdentifier("cassandra", "cassandra", "table1")
      val column = new Column("column1", tableId)
      implicit val ordering = new Ordering[String] { def compare(a: String, b: String) = {0}}
      val columnOrdering =  new ColumnOrdering(column, false)
      val domaine = new RangeValueDomain(column, Some(new Bound(true, 0)), Some(new Bound(true, 100)))

    val query = new DomainQuery(
      java.util.UUID.randomUUID,
      "queryspace1", 
      0,
      Set(column),
      tableId,
      List(domaine),
      None,
      Some(columnOrdering),
      Some(QueryRange(None, None, None)))
      
       // Create lucene code.
      val queryString = DomainToJSONLuceneQueryMapper.getLuceneColumnsQueryMapping(query, List(domaine), tableId)
      Assert.assertEquals(queryString.get, " lucene=\'{  filter : { type : \"range\", field : \"column1\",  lower: \"0\" , include_lower: \"true\" , upper: \"100\" , include_upper: \"true\"  } ,  sort : { fields : [ { field : \"column1\" , reverse : false } ] }  }' ")      
    }
  }
}