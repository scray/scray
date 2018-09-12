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

import java.nio.file.Files

import scray.hdfs.io.index.format.sequence.BlobFileReader
import scray.hdfs.io.index.format.sequence.IdxReader
import scray.hdfs.io.index.format.sequence.types.BlobInputStream
import java.io.File
import org.apache.commons.io.FileUtils
import scala.io.Source

object CopyFromSequneceFile {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage copyFromSequenceFile SOURCE DEST")
      println("Example parameters:\n  hdfs://10.0.0.1/SequenceTest/testFile /home/stefan/Downloads/testFile.pdf")
    } else {

      val sourceFile = args(0)
      val destination = args(1)

      val blobReader = new BlobFileReader(s"${sourceFile}.blob")
      
      val idxReader = new IdxReader(s"${sourceFile}.idx")
      if(idxReader.hasNext) {
        val idx = idxReader.next().get
        val stream = new BlobInputStream(blobReader, idx)

        FileUtils.copyInputStreamToFile(stream, new File(destination));
        
      }

      println(s"Write content of file ${sourceFile} to destination ${destination}.")

      println("\n============================================================")
      println(s"  New files can be found in ${destination}")
      println(  "============================================================")
    }

  }
}