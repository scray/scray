package org.scray.example.data

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import org.junit.Assert

class JsonFacilityParserSpecs extends WordSpec with LazyLogging {
  
  "JsonFacilityParser" should {
    " pars arry with one facility element " in {
      val parser = new JsonFacilityParser
      
      Assert.assertTrue(parser.jsonReader("[{\"equipmentnumber\":10216020,\"type\":\"ELEVATOR\",\"description\":\"zu Gleis 3/4 (S-Bahn)\",\"geocoordX\":13.2871746,\"geocoordY\":52.6697995,\"state\":\"INACTIVE\",\"stationnumber\":2832}]").isDefined)
    }
  }
  
}