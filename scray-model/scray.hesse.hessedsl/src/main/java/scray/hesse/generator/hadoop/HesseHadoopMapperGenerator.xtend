package scray.hesse.generator.hadoop

import java.util.List
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.generator.hadoop.HesseHadoopMapperFiltering.VarAndFilters
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.SupportedDBMSSystems

class HesseHadoopMapperGenerator {
	
	val writableGenerator = new HesseHadoopWritableGenerator
	
	/**
	 * generates Mapper code
	 */
	def generateMapper(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {		
		
		val VarAndFilters filtering = (new HesseHadoopMapperFiltering).generateFilteringAndFunctionApplication(state, header, view, bodyStatements)
		'''
		«writableGenerator.generateInputDBWritable(state, header, view, bodyStatements)»
		
		/**
		 * Mapper implementation.
		 * Implements a simple mapping step with serialization using kryo.
		 */
		class «header.modelname + view.name»Mapper extends MapReduceBase with Mapper[«IF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.CASSANDRA)
			»Long, Row«
		ELSEIF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.MYSQL) || header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.ORACLE)
			»LongWritable, «header.modelname + view.name»DBInputWritableRow«
		ENDIF», «header.modelname + view.name»KeyBytesWritable, «header.modelname + view.name»RowBytesWritable] {
			
			«state.protectedRegions.protect("MAPPER_CLASS_VARS", "// place additional defs variables and initialization here", false)»
			
			override def configure(conf: JobConf): Unit = {
				«state.protectedRegions.protect("MAPPER_CONFIGURE", "// place additional mapper configure code here", false)»
			}
			
			/**
			 * map method that gets executed for every input Row
			 */
			override def map(«IF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.CASSANDRA)
					»key: Long, value: Row«
				ELSEIF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.MYSQL) || header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.ORACLE)
					»key: LongWritable, origvalue: «header.modelname + view.name»DBInputWritableRow«
				ENDIF», output: OutputCollector[«header.modelname + view.name»KeyBytesWritable, «header.modelname + view.name»RowBytesWritable], reporter: Reporter): Unit = {
				reporter.progress()
				reporter.incrCounter("Mapper", "Maps", 1)
				«IF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.MYSQL) || header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.ORACLE)»
				// extract Cassandra-like Row from input data
				val value = origvalue.getRow.orNull
				«ENDIF»
				// create filters
				«IF filtering != null»
					«filtering.code»
					var filterResult = «filtering.varname»
				«ENDIF»
				«state.protectedRegions.protect("MAPPER_MAP", "// place additional mapper map code here", false)»
				«/* TODO: we need to: filter and group */»
				outValue.setSize
				«IF filtering != null»
					
					if(filterResult) {
						ouput.collect(outKey, outValue)
					}
				«ELSE»
					ouput.collect(outKey, outValue)
				«ENDIF»
			}
			
			override def close(): Unit = {
				«state.protectedRegions.protect("MAPPER_CLOSE", "// place additional mapper closing code here", false)»
			}
		}
		'''
	}
}
