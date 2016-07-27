package scray.cassandra

import scray.common.exceptions.ScrayException
import java.util.UUID

class CassandraTableNonexistingException(tableName: String) extends ScrayException(ExceptionIDs.tableNonExistingInCassandraID, 
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"$tableName : does not exist in Cassandra. Please create table before (re-)loading Scray.") with Serializable
