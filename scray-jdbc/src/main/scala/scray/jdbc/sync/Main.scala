package scray.jdbc.sync

object Main {
  
  def main(args: Array[String]): Unit = {
    val f = new JDBCDbSessionImpl("jdbc:hive2://10.11.22.31:10000/", "", "");
  }
}