package scray.hdfs.compaction.conf

import org.yaml.snakeyaml.Yaml
import com.typesafe.scalalogging.LazyLogging
import org.yaml.snakeyaml.error.YAMLException
import java.util.LinkedHashMap

class YamlConfigurationParser extends LazyLogging {
  
  def parse(txt: String): Option[CompactionJobParameter] = {
    Some(CompactionJobParameter())
  }
  
}