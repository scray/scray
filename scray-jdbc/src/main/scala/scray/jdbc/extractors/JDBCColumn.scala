package scray.jdbc.extractors

import scray.querying.description.TableIdentifier

/**
 * case class which represents columns in JDBC databases
 */
case class JDBCColumn(name: String, ti: TableIdentifier, typ: Int)