// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

class HDFSURLUndefinedException()
    extends ScrayException(ExceptionIDs.urlMissingExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"hdfs is specified as a DBMS system, at least a HDFS-URL to connect must be provided") with Serializable

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

class UnknownBooleanValueException(supposedbool: String)
    extends ScrayException(ExceptionIDs.unknownBooleanValueExceptionID,
        UUID.nameUUIDFromBytes(Array[Byte](0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)),
        s"Supposed boolean value $supposedbool is not parsable.") with Serializable
