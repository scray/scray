package scray.loader.tools


import org.junit.runner.RunWith
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.loader.configparser.ScrayConfigurationParser
import scray.querying.description.TableIdentifier
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.querybuilder.QueryBuilder

@RunWith(classOf[JUnitRunner])
class CassandraIndexStatementGeneratorImplSpecs extends WordSpec with LazyLogging {
   "CassandraIndexStatementGenerator " should {
     "create index statement for one column " in {
       val statementGenerator = new CassandraIndexStatementGeneratorImpl
       val configurationString = statementGenerator.getIndexString(TableIdentifier("cassandra", "ks", "cf1"), List("col1", "col2"))
       
       assert(configurationString == "CREATE INDEX ON \"ks\".\"cf1\" (\"col1\", \"col2\" );")
    }
    "create index statement for multiple columns " in {
       val statementGenerator = new CassandraIndexStatementGeneratorImpl
       val configurationString = statementGenerator.getIndexString(TableIdentifier("cassandra", "ks", "cf1"), List("col1"))
       
       assert(configurationString == "CREATE INDEX ON \"ks\".\"cf1\" (\"col1\" );")
    }
   }
}