package scray.hesse.generator.hadoop

import java.util.List
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement

class HesseHadoopReducerGenerator {
	
	/**
	 * generates Reducer code
	 */
	def generateReducer(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		class «header.modelname + view.name»Reducer extends MapReduceBase with Mapper[BytesWritable, BytesWritable, NullWritable, NullWritable] {
			
			val outKey = new BytesWritable
			val outValue = new BytesWritable
			
			override def map(key: Long, value: Row, output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter) = {
				reporter.progress()
				reporter.incrCounter("Mapper", "Maps", 1)
				outValue.setSize
				ouput.collect(outKey, outValue)
			}
		}
		'''	
	}
}