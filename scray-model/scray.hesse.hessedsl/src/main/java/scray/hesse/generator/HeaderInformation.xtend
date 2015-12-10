package scray.hesse.generator

import com.google.common.base.Function
import com.google.common.base.Optional
import java.util.List
import scray.hesse.hesseDSL.AbstractHeaderElement
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.DBMSRefTable
import scray.hesse.hesseDSL.DefaultDBIDElement
import scray.hesse.hesseDSL.DefaultDBMSHeaderElement
import scray.hesse.hesseDSL.Header
import scray.hesse.hesseDSL.SupportedDBMSSystems
import scray.hesse.hesseDSL.SupportedPlatforms
import scray.hesse.hesseDSL.MaterializedViewStatement

/**
 * the header information will be parsed and inserted into
 * the resulting object
 */
class HeaderInformation {

	new(Header header) {
		// this is safe, because this name has to be definitely set
		modelname = header.name.name
		// target platform to decide what type of classes will be generated
		platform = if(StaticUtilities::isSet(header, "platform")) header.platform.name else HesseDSLGenerator::DEFAULT_PLATFORM
		// queryspace to generate data in
		queryspace = if(StaticUtilities::isSet(header, "queryspace")) header.queryspace.name else modelname
		// default storage database system
		defaultdbsystem = getHeaderElement(header, typeof(DefaultDBMSHeaderElement)).transform(
			new Function<DefaultDBMSHeaderElement, SupportedDBMSSystems>() {
				public override def SupportedDBMSSystems apply(DefaultDBMSHeaderElement e) { e.name }
			}).or(Optional.fromNullable(HesseDSLGenerator::DEFAULT_DBMS)).get
		// default db-id
		dbid = getHeaderElement(header, typeof(DefaultDBIDElement)).transform(
			new Function<DefaultDBIDElement, String>() {
				public override def String apply(DefaultDBIDElement e) { e.name }
			}).or(Optional.fromNullable("")).get
	}

	/**
	 * return the header element with the given type
	 */
	private def <T extends AbstractHeaderElement> Optional<T> getHeaderElement(Header header, Class<T> type) {
		if (StaticUtilities::isSet(header, "headerElements")) {
			Optional.fromNullable(header.headerElements.findFirst[e | e.class == type] as T)
		} else {
			Optional.absent
		}
	}
	
	/**
	 * evaluate whether the requested DBMS is used anywhere in the given BodyStatements
	 */
	public def isDBMSUsed(List<BodyStatement> bodyStatements, SupportedDBMSSystems dbms) {
		if(dbms == defaultdbsystem) true else {
			bodyStatements.findFirst[bodyStatement | 
				if(bodyStatement instanceof MaterializedViewStatement) {
					val matview = bodyStatement as MaterializedViewStatement
					if(StaticUtilities::isSet(matview, "dbms") && matview.dbms == dbms) true else false
				} else false || bodyStatement.eAllContents.findFirst [ content | 
				content.getClass == typeof(DBMSRefTable) && (content as DBMSRefTable).dbms == dbms] != null] != null
		}
	}

	// the name of the model which we use for the jobs name
	public val String modelname

	// the platform which has been specified in the script
	public val SupportedPlatforms platform

	// the default database management system to assume as default 
	public val SupportedDBMSSystems defaultdbsystem
	
	// the name of the queryspace
	public val String queryspace
	
	// the name of the database to use
	public val String dbid
}