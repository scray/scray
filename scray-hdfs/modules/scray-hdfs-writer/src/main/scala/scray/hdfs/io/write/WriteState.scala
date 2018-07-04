package scray.hdfs.io.write


class WriteState {
  var isCompleted: Boolean = false
  var completedState: WriteResult = null
}