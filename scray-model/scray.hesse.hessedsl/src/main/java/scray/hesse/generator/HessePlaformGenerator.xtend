package scray.hesse.generator

import java.util.List
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement

/**
 * platform specific generators implement this interface
 */
interface HessePlaformGenerator {
	def void doGenerate(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements);
}