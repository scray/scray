package scray.cassandra.tools

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.cassandra.tools.api.Column
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.TableIdentifier
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.DataType.Name._
import com.datastax.driver.core.DataType.Name
import scray.cassandra.tools.types.ScrayColumnType
import scray.cassandra.tools.types.LuceneColumnTypes
import scray.cassandra.tools.types.ScrayColumnType

@RunWith(classOf[JUnitRunner])
class LuceneColumnTypesSpecs extends WordSpec with LazyLogging {
  "LuceneCloumnTypes " should {
    "be created from ScrayColumnType string" in {
      
      val scrayStringType = ScrayColumnType.String("column1")
      
      assert(LuceneColumnTypes.getLuceneType(scrayStringType) === LuceneColumnTypes.String("column1", None))
      assert(LuceneColumnTypes.getLuceneType(scrayStringType).toString() === "column1\t {type: \"string\" }")
    }
    "be created from ScrayColumnType long" in {
      
      val scrayLongType = ScrayColumnType.Long("column1")
      
      assert(LuceneColumnTypes.getLuceneType(scrayLongType) === LuceneColumnTypes.Long("column1", None))
      assert(LuceneColumnTypes.getLuceneType(scrayLongType).toString() === "column1\t {type: \"long\" }")
    }
  }
}