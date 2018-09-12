package scray.hdfs.index

import scray.hdfs.index.HDFSBlobResolver._
import org.scalatest.WordSpec
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.junit.Assert
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import scala.collection.mutable.MutableList
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow
import org.scalatest.Tag
import scray.querying.description.TableIdentifier
import org.apache.hadoop.io.Text
import com.typesafe.scalalogging.LazyLogging

class HDFSBlobResolverSpecs extends WordSpec with LazyLogging  {
  "HDFSBlobResolver" should {
    " store index in memory " in {
      val key      = "key1".getBytes
      val filename = "file1"
      
      HDFSBlobResolver.putIntoIndexCache(new ArrayBytes(key),filename, 42)
      Assert.assertEquals(HDFSBlobResolver.getCachedIdxPos(new ArrayBytes(key)), Some(("file1", 42)))
    }
    "create hash" in {
      val hash1 = HDFSBlobResolver.computeHash(new Text("a"), TableIdentifier("1", "2", "3"))    
      
      HDFSBlobResolver.putIntoIndexCache(new ArrayBytes(hash1), "f1",  42)
      Assert.assertEquals(HDFSBlobResolver.getCachedIdxPos(new ArrayBytes(hash1)), Some(("f1", 42)))
    }
  }
  
}