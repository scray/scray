package scray.storm.index

import shapeless.HNil
import com.websudos.phantom.CassandraPrimitive
import shapeless._
import shapeless.ops.hlist._
import shapeless.UnaryTCConstraint._
import com.websudos.phantom._
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.store.StorehausCassandraVersionedStore.BatchIDAsCassandraPrimitive

package object time {
  type TupleTypeRK = (Int, Int)
  type TupleTypeCK = (Long, BatchID)
  type TupleTypeKey = (TupleTypeRK, TupleTypeCK)
  type SerializersTypeRK = CassandraPrimitive[Int] :: CassandraPrimitive[Int] :: HNil
  type SerializersTypeCK = CassandraPrimitive[Long] :: CassandraPrimitive[BatchID] :: HNil
  type HListTypeRK = Int :: Int :: HNil
  type HListTypeCK = Long :: BatchID :: HNil
  val serializersRK = implicitly[CassandraPrimitive[Int]] :: implicitly[CassandraPrimitive[Int]] :: HNil
  val serializersCK = implicitly[CassandraPrimitive[Long]] :: implicitly[CassandraPrimitive[BatchID]] :: HNil
  val columnNamesRK = List("Year", "Spread")
  val columnNamesCK = List("Time", "BatchID")
  val valueColumnName = "value"
}