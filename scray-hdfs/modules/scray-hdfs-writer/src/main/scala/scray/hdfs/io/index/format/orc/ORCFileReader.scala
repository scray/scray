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

package scray.hdfs.io.index.format.orc

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.slf4j.LoggerFactory
import org.apache.orc.Reader
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.Reader.Options
import org.apache.orc.RecordReader

class ORCFileReader {

  private var reader: Reader = null;
  val log = LoggerFactory.getLogger("scray.hdfs.index.format.orc.ORCFileReader");

  private final val ID_COLUMN = "id";
  private final val COLUMNS: Array[String] = Array("id");

  def this(path: String) = {
    this

    val hadoopConfig = new Configuration();

    hadoopConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hadoopConfig.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    try {
      reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(hadoopConfig));
    } catch {
      case e: IllegalArgumentException => {
        log.error(s"Incorrect path parameter ${path}, Message: ${e.getMessage}");
        e.printStackTrace();
      }
      case e: IOException => {
        log.error("IOException while acessing orc file {}, Message: {}");
        e.printStackTrace();
      }
    }
  }

  def get(key: String): Option[Array[Byte]] = {

    // Filter elements based on ORC statistics
    val sarg: SearchArgument = SearchArgumentFactory.newBuilder().startAnd()
      .equals(ID_COLUMN, org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Type.STRING, key).end().build();
    val options = new Options().searchArgument(sarg, COLUMNS);

    var result: Option[Array[Byte]] = None

    try {
      val readStartTime = System.currentTimeMillis();

      val batch = reader.getSchema().createRowBatch();
      val rows = reader.rows(options);

      var readRowCounter = 0;
      var found = false;

      while (rows.nextBatch(batch) && !found) {
        for (rowNr <- 0 to batch.size if !found) {
          val readId = batch.cols(0).asInstanceOf[BytesColumnVector];
          val time = batch.cols(1).asInstanceOf[LongColumnVector];
          val data = batch.cols(2).asInstanceOf[BytesColumnVector];

          if (readId.toString(rowNr).equals(key)) {
            found = true;

            // Copy data to result set
            val dataStartPos = data.start(rowNr)
            val dataLength = data.length(rowNr)

            val requestedData = new Array[Byte](dataLength);
            System.arraycopy(data.vector(rowNr), dataStartPos, requestedData, 0, dataLength);

            result = Some(requestedData);
          }

          readRowCounter += 1;
        }
      }

      rows.close();

      val readEndTime = System.currentTimeMillis();
      log.debug("Consumed time: " + (readEndTime - readStartTime) + "\t Read rows: " + readRowCounter);
      
      readRowCounter = 0;

    } catch {
      case e: IOException => {
        log.error(s"Error while reading row from orc file. ${e.getMessage()}");
        e.printStackTrace();
      }
    }

    return result;
  }
}
