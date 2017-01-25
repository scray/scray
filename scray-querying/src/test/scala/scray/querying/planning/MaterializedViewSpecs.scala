package scray.querying.planning

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import java.util.ArrayList
import scray.querying.queries.SimpleQuery
import scray.querying.description.And
import scray.querying.description.Column
import scray.querying.description.Equal
import scray.querying.description.TableIdentifier
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.MaterializedViewQueryException
import scray.querying.description.internal.QueryDomainParserExceptionReasons
import scray.querying.description.Or
import scray.querying.description.internal.Domain
import org.scalatest.Assertions._
import scala.collection.mutable.HashMap
import scray.querying.description.SimpleRow
import scray.querying.description.internal.MaterializedView
import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import scray.querying.queries.SimpleQuery

@RunWith(classOf[JUnitRunner])
class MaterializedViewSpecs extends WordSpec {
  "Planer " should {
    " transform AND-Query to mv query" in {

      val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
      val query = SimpleQuery("", ti,
        where = Some(
          And(
            Equal(Column("col4", ti), 4),
            Equal(Column("col8", ti), 2)
           )
         )
        )

      val flatQueries = Planner.distributiveOrReductionToConjunctiveQuery(query)

      assert(flatQueries.size == 1)
      val domains = Planner.qualifyPredicates(flatQueries.head).get
      val mvDomaine = Planner.getMvQuery(domains, query, ti)

      assert(mvDomaine.column.columnName == "key")
      assert(mvDomaine.value == "4_2")

    }
    " transform OR-Query to mv query" in {

      val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
      val query = SimpleQuery("", ti,
        where = Some(
          Or(
             Equal(Column("col4", ti), 4),
             Equal(Column("col8", ti), 2)
           )
          )
        )

      val flatQueries = Planner.distributiveOrReductionToConjunctiveQuery(query)

      assert(flatQueries.size == 2)
      val domains1 = Planner.qualifyPredicates(flatQueries.head).get
      val mvDomaine1 = Planner.getMvQuery(domains1, query, ti)

      assert(mvDomaine1.column.columnName == "key")
      assert(mvDomaine1.value == "4")
      
      val domains2 = Planner.qualifyPredicates(flatQueries.tail.head).get
      val mvDomaine2 = Planner.getMvQuery(domains2, query, ti)

      assert(mvDomaine2.column.columnName == "key")
      assert(mvDomaine2.value == "2")
    }
  }

}