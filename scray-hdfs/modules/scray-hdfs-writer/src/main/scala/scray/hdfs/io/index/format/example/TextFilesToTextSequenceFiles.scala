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

import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextText
import scray.hdfs.io.index.format.sequence.SequenceFileWriter

object TextToTextSequenceFiles {

  def main(args: Array[String]) {

    val writer = new SequenceFileWriter("hdfs://127.0.0.1/123/TdextToTextSequenceFile", new OutputTextText, true)

    writer.insert("id1", """{"msg_id": 1, "msg": "msg1"}""")
    writer.insert("id2", """{"msg_id": 2, "msg": "msg2"}""")
    writer.insert("id3", """{"msg_id": 3, "msg": "msg3"}""")
    
    writer.close

  }
}