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

package scray.hdfs.io.index.format.example

import java.io.FileInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.SequenceFile.Writer

import scray.hdfs.io.index.format.sequence.types.IndexValue
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.FileOutputStream
import scray.hdfs.io.index.format.sequence.types.Blob
import scray.hdfs.io.index.format.sequence.SequenceFileWriter
import scray.hdfs.io.index.format.sequence.IdxReader
import scray.hdfs.io.index.format.sequence.ValueFileReader
import scala.collection.mutable.HashMap
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable

/**
 * Example application to copy files from local file system to SequenceFile.
 */
object CopyToSequenceFile {
  
  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage copyToSequenceFile SOURCE DEST")
      println("Example parameters:\n  /home/stefan/Downloads/testFile.pdf  hdfs://10.0.0.1/SequenceTest/ ")
    } else {

      val sourceFile = args(0)
      val destination = args(1)

      val destWriter = new SequenceFileWriter(destination, new Configuration, None, new OutputTextBytesWritable)

      val sourceReader = new FileInputStream(sourceFile)
      val filename = sourceFile.split(System.getProperty("file.separator")).last
      
      println(s"Write content of file ${sourceFile} to destination ${destination}.\n File can be queried with key ${filename}")
      
      val startTime = System.currentTimeMillis() 
//      val writtenBytes = destWriter.insert(filename, System.currentTimeMillis(), sourceReader, 5 * 1024 * 1024)
      val writtenBytes = destWriter.insert(filename, System.currentTimeMillis(), sourceReader)

      destWriter.close
      
      println("\n============================================================")
      println(s"  New files can be found in ${destination}")
      println(s"  Key to query content: ${filename}")
      println(s"  ${writtenBytes/1024/1024}MB written in ${(System.currentTimeMillis() - startTime)/1000}s")
      println(  "============================================================")
    }

  }
}