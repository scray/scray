package scray.cassandra.tools

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.cassandra.tools.types.{LuceneColumnTypes, ScrayColumnTypes}

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