package scray.hesse.generator

import java.io.InputStream
import java.util.HashMap
import java.util.Map
import java.util.NoSuchElementException
import java.util.Scanner
import java.util.regex.Pattern
import org.eclipse.core.internal.resources.ResourceException
import org.eclipse.core.resources.IFile

/**
 * class that emulates protected regions in xtend
 */
class ProtectedRegions {
	
    private var Map<String, String> protectedRegions = new HashMap<String, String>

    /**
     * creates a new protected region or uses the content
     * of the previously existing one
     */
    def protect(String id, String relevantContent, boolean xml) {
        val content = new StringBuilder
        if(xml) {
        	content.append("<!--")
        } else {
        	content.append("/*")
        }
        content.append(" PROTECTED REGION START ##")
        content.append(id)
        content.append("##: ")
        if(xml) {
        	content.append("-->")
        } else {
        	content.append("*/")
        }        
        if(protectedRegions.get(id) != null) {
            // remove last newLine, if there is one and add content
            var protRegContentComplete = protectedRegions.get(id)
            var String protRegContent = null
            if(protRegContentComplete.endsWith(System::getProperty("line.separator"))) {
                protRegContent = protRegContentComplete.substring(0, protRegContentComplete.length() - System::getProperty("line.separator").length)
            } else {
                protRegContent = protRegContentComplete
            }
            content.append(protRegContent)

        } else {
            StaticUtilities::nl(content)
            content.append(relevantContent)
            StaticUtilities::nl(content)
        }
        if(xml) {
        	content.append("<!--")
        } else {
        	content.append("/*")
        }
        content.append(" PROTECTED REGION END ")
        if(xml) {
        	content.append("-->")
        } else {
        	content.append("*/")
        }        
        StaticUtilities::nl(content)
        return content
    }

	/**
	 * convenience method to read the protected regions 
	 */
	def readProtectedRegionsFromFile(String name, GeneratorState state, boolean xml) {
		readProtectedRegionsFromFile(state.getSystemFile(name), xml)
	}

    /**
     * reads all protected regions from file into the
     * field protectedregions of type map
     */
    def readProtectedRegionsFromFile(IFile file, boolean xml) {
        protectedRegions = new HashMap<String, String>
        try {

            var InputStream istream = file.contents
            var String resultingString
            try {
                resultingString = new Scanner(istream).useDelimiter("\\A").next();
            } catch (NoSuchElementException e) {
                resultingString = "";
            }
            val pattern = if(xml) {
            	Pattern::compile("PROTECTED REGION START ##(.+?)##: -->(.*?)<!-- PROTECTED REGION END", Pattern::DOTALL)
            } else {
            	Pattern::compile("PROTECTED REGION START ##(.+?)##: \\*/(.*?)/\\* PROTECTED REGION END", Pattern::DOTALL)
            }
            val matcher = pattern.matcher(resultingString)
            while(matcher.find()) {
                val normalizeSpaces = matcher.group(2).split("\\n")
                val b = new StringBuilder
                for(c : normalizeSpaces) {
                    b.append(c.trim)
                    StaticUtilities::nl(b)
                }
                protectedRegions.put(matcher.group(1), b.toString)
            }
            istream.close
        } catch(ResourceException e) {
            // do not fill any map -> file did not exist
        }
    }
}
