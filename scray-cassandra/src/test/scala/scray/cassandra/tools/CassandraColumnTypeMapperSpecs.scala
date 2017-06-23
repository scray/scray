package scray.cassandra.tools

import org.junit.runner.RunWith
import com.typesafe.scalalogging.LazyLogging
import com.datastax.driver.core.DataType
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.cassandra.tools.types.{CassandraColumnTypeMapper, ScrayColumnTypes}

@RunWith(classOf[JUnitRunner])
class CassandraColumnTypeMapperSpecs extends WordSpec with LazyLogging {
   "CassandraColumnTypeMapper" should {
     "find scray colum type for a given cassandra type " in {
       assert(CassandraColumnTypeMapper.findScrayType("col1", DataType.text())  == Some(ScrayColumnTypes.String("col1")))
       assert(CassandraColumnTypeMapper.findScrayType("col1", DataType.cint())  == Some(ScrayColumnTypes.Integer("col1")))
       assert(CassandraColumnTypeMapper.findScrayType("col1", DataType.bigint())  == Some(ScrayColumnTypes.Long("col1")))
     }
   }
  
}