package scray.cassandra.tools

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.TableIdentifier
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.DataType.Name._
import com.datastax.driver.core.DataType.Name
import scray.cassandra.tools.types.ScrayColumnTypes
import scray.cassandra.tools.types.LuceneColumnTypes
import scray.cassandra.tools.types.ScrayColumnTypes

@RunWith(classOf[JUnitRunner])
class LuceneColumnTypesSpecs extends WordSpec with LazyLogging {
  "LuceneCloumnTypes " should {
    "be created from ScrayColumnType string" in {
      
      val scrayStringType = ScrayColumnTypes.String("column1")
      
      assert(LuceneColumnTypes.getLuceneType(scrayStringType) === Some(LuceneColumnTypes.String("column1", None)))
      assert(LuceneColumnTypes.getLuceneType(scrayStringType).getOrElse("").toString() === "string")
    }
    "be created from ScrayColumnType integer" in {
      
      val scrayLongType = ScrayColumnTypes.Integer("column1")
      
      assert(LuceneColumnTypes.getLuceneType(scrayLongType) === Some(LuceneColumnTypes.Integer("column1", None)))
      assert(LuceneColumnTypes.getLuceneType(scrayLongType).getOrElse("").toString() === "integer")
    }
    "be created from ScrayColumnType long" in {
      
      val scrayLongType = ScrayColumnTypes.Long("column1")
      
      assert(LuceneColumnTypes.getLuceneType(scrayLongType) === Some(LuceneColumnTypes.Long("column1", None)))
      assert(LuceneColumnTypes.getLuceneType(scrayLongType).getOrElse("").toString() === "long")
    }
  }
}