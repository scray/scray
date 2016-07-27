package scray

package object cassandra {
  object ExceptionIDs {
    // 300+ = query space registration errors
    val tableNonExistingInCassandraID: String = "Scray-Cassandra-Nonexistingtable-300"
  }
}