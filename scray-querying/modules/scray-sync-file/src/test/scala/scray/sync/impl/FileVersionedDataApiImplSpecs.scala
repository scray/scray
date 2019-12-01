package scray.sync.impl

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import junit.framework.Assert

class FileVersionedDataApiImplSpecs extends WordSpec {
  "FileVersionedData" should {
    "update versions " in {
      val fs = new FileVersionedDataApiImpl("null")

      fs.updateVersion("online", "key", 1, "")
      Assert.assertEquals(fs.getLatestVersion("online", "key").get.version, 1)

      fs.updateVersion("online", "key", 2, "")
      Assert.assertEquals(fs.getLatestVersion("online", "key").get.version, 2)
    }
  }
}