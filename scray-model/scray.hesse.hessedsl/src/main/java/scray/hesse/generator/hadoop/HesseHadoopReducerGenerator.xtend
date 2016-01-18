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
		class «header.modelname + view.name»Reducer extends MapReduceBase with Reducer[«header.modelname + view.name»KeyBytesWritable, «header.modelname + view.name»RowBytesWritable, NullWritable, NullWritable] {

			«state.protectedRegions.protect("REDUCER_CLASS_VARS", "// place additional defs variables and initialization here", false)»
			
			override def configure(conf: JobConf): Unit = {
				«state.protectedRegions.protect("REDUCER_CONFIGURE", "// place additional reducer configure code here", false)»
			}
			
			val outKey = new «header.modelname + view.name»KeyBytesWritable
			val outValue = new «header.modelname + view.name»RowBytesWritable
			
			override def reduce(key: «header.modelname + view.name»KeyBytesWritable, value: JIterator[«header.modelname + view.name»RowBytesWritable], output: OutputCollector[NullWritable, NullWritable], reporter: Reporter): Unit = {
				reporter.progress()
				reporter.incrCounter("Reducer", "Reduces", 1)
				
			}
			
			override def close(): Unit = {
				«state.protectedRegions.protect("REDUCER_CLOSE", "// place additional reducer closing code here", false)»
			}
			
		}
		'''	
	}
}