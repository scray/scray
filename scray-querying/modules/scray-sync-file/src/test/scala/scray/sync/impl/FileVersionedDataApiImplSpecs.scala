package scray.sync.impl

import org.junit.runner.RunWith
import org.scalatest.WordSpec

class FileVersionedDataApiImplSpecs extends WordSpec {
  "FileVersionedData" should {
    "write data out " in {
      val fs = new FileVersionedDataApiImpl("")
      fs.writeNextVersion("", "", 1, "")
      fs.getLatestVersion("", "")
      }
    }
}