package scray.loader

import scray.common.exceptions.ScrayException
import java.util.UUID
import scray.querying.description.TableIdentifier

class IndexConfigException(table: TableIdentifier) 
    extends ScrayException(ExceptionIDs.indexConfigExceptionID, 
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)), 
        s"index configuration for system ${table.dbSystem} db ${table.dbId} table ${table.tableId} is wrong") with Serializable

        
class PropertyNException(propertyName: String, propertyNClass: String) 
    extends ScrayException(ExceptionIDs.propertySetExceptionID, 
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)), 
        s"$propertyNClass : property value of $propertyName cannot be converted into Set of Tuples of Strings") with Serializable

class CassandraHostsUndefinedException()
    extends ScrayException(ExceptionIDs.hostMissingExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"cassandra is specified as a DBMS system, at least one host to connect must be provided") with Serializable

class JDBCURLUndefinedException()
    extends ScrayException(ExceptionIDs.urlMissingExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"jdbc is specified as a DBMS system, at least a JDBC-URL to connect must be provided") with Serializable

class CassandraClusterSpecificationException()
    extends ScrayException(ExceptionIDs.hostMissingExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"cassandra is specified as a DBMS system, at least one host to connect must be provided") with Serializable

class DBMSUndefinedException(dbms: String, queryspace: String)
    extends ScrayException(ExceptionIDs.dbmsUndefinedExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"DBMS system $dbms has been referenced in queryspace $queryspace, but it hasn't been defined in the config") with Serializable

class UnsupportedMappingTypeException(mapping: String, queryspace: String)
    extends ScrayException(ExceptionIDs.mappingUnsupportedExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"In queryspace $queryspace mapping $mapping has been requested, but it is not supported") with Serializable

class UnknownTimeUnitException(supposedunit: String)
    extends ScrayException(ExceptionIDs.unknownTimeUnitExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"Supposed time unit $supposedunit is not parsable.") with Serializable
