package scray.querying.sync.cassandra

import com.websudos.phantom.CassandraPrimitive
import scray.querying.sync.types.DBColumnImplementation


object CassandraImplementation {
  implicit def genericCassandraColumnImplicit[T](implicit cassImmplicit: CassandraPrimitive[T]): DBColumnImplementation[T] = new DBColumnImplementation[T]  {
    override def getDBType: String = {
      cassImmplicit.cassandraType
    }
  }
}