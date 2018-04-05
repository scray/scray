package scray.hdfs.index.format.example

import java.io.FileInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.SequenceFile.Writer

import scray.hdfs.index.format.sequence.types.IndexValue
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.FileOutputStream
import scray.hdfs.index.format.sequence.types.Blob
import scray.hdfs.index.format.sequence.BinarySequenceFileWriter
import scray.hdfs.index.format.sequence.IdxReader
import scray.hdfs.index.format.sequence.BlobFileReader
import scala.collection.mutable.HashMap
import java.io.PrintWriter
import scala.io.Source


object Main {
  def main(args: Array[String]) {
    
    
    var loader = getClass().getClassLoader();
    while (loader != null) {
      System.out.println ("Affe HDFS" + loader.getClass().getName());
      loader = loader.getParent();
    }
    
    val aaa = new org.apache.hadoop.conf.Configuration
    
//    val writer = new PrintWriter("/tmp/testFile");
//    
//    for(i <- 0 to 9000000) {
//      writer.println(i)
//    }
//    
//    writer.close()
    
//printDiff
    //write("/home/stefan/Downloads/BIS-6.5.2_SP54_Rev3.0.iso")
//
//    readFile("testFile", "/tmp/ff.out")
//    
//    val a: org.apache.hadoop.conf.Configuration = null
  }
  
  
  
  def printDiff = {
        val newFile = Source.fromFile("/tmp/testFile.out").getLines
    for (line <- Source.fromFile("/tmp/testFile").getLines) {
      val outLine = newFile.next()
      if(!outLine.equals(line)) {
        println(outLine + "\t" + line + "\t" + (outLine.toLong - line.toLong))
      }
    }
  }
  
  def write(fileName: String) = {
    val writer = new BinarySequenceFileWriter("/tmp/spdf/", new Configuration, None)   
    val file = new FileInputStream(fileName)

    writer.insert("BIS-6.5.2_SP54_Rev3.0.iso", System.currentTimeMillis(), file)
    writer.close
  }
  
  def readIdx() = {
    val idxReader = new IdxReader("hdfs://10.11.22.34/BIS_ISO/SP58.idx", new Configuration, None)
    
    val idx = new HashMap[String, IndexValue]
    
    while(idxReader.hasNext) {
      idxReader.next().map{ idxValue =>
       idx.put(idxValue.getKey, idxValue)
      }
    }
    
    idx
  }
  
  
  def readFile(key: String, output: String) = {
    val outStream =  new FileOutputStream(output)
    
    val blobReader = new BlobFileReader("hdfs://10.11.22.34/BIS_ISO/SP58.blob", new Configuration, None)
    
    val idx = readIdx()
    val possiton: IndexValue = idx.get(key).get

    //blobReader.printBlobs(possiton.getPosition)
    var lastPossition = possiton.getPosition
    for(i <- 0 to possiton.getBlobSplits) {
      blobReader.getNextBlob(key, i, lastPossition)
      .map(blob => {
        lastPossition = blob._1
        println(s"${i} offset ${blob}")
        outStream.write(blob._2.getData)
       })
    }
    
    outStream.close()
  }
  
}