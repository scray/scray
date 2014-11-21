package scray.querying.source

import org.mapdb._

class LazyCachingSource {

  def test = {
    val db = DBMaker.newMemoryDirectDB().compressionEnable().make
    db.createHashMap("bla").make
  }
  
}