package scray.client.test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ScrayJdbcAccessParser
{
    private static enum OPTIONS {
        help("h"), statement("s"), numsets("n"), url("u"), timeout("o"), fetchsize("f"), dots("d");
        private String arg = null; 
        private OPTIONS(String arg) {
            this.arg = arg;
        }
        public String getArg() {
            return arg;
        }
    }
    
    private static Options setupCLIOptions() {
        Options options = new Options();
        Option url = new Option(OPTIONS.url.getArg(), "url", true, 
                        "database-url in scray-url format, i.e. jdbc:scray:<stateless|stateful>://<host>:<port>/<dbsystem>/<dbid>/<queryspace>");
        url.setArgName("URL");
        url.setRequired(false);
        options.addOption(url);
        Option numsets = new Option(OPTIONS.numsets.getArg(), "numsets", true, "number of result sets to fetch, -1 for all (default) -> multiply with f");
        numsets.setArgName("NUMBER");
        numsets.setRequired(false);
        options.addOption(numsets);
        Option statement = new Option(OPTIONS.statement.getArg(), "statement", true, "the query statement.");
        statement.setArgName("STATEMENT");
        statement.setRequired(false);
        options.addOption(statement);
        Option timeout = new Option(OPTIONS.timeout.getArg(), "timeout", true, "timeout, default = 60");
        timeout.setArgName("SECONDS");
        timeout.setRequired(false);
        options.addOption(timeout);
        Option fetchsize = new Option(OPTIONS.fetchsize.getArg(), "fetchsize", true, "fetch size for each result set, default = 50");
        fetchsize.setArgName("SIZE");
        fetchsize.setRequired(false);
        options.addOption(fetchsize);
        Option help = new Option(OPTIONS.help.getArg(), "help", false, "print usage information");
        help.setRequired(false);
        options.addOption(help);
        Option dots = new Option(OPTIONS.dots.getArg(), "dots", false, "print a dot every 10000 results instead of content");
        dots.setRequired(false);
        options.addOption(dots);
        return options;
    }
    
    private static boolean interpretCommandLine(CommandLine cl, ScrayJdbcAccess jdbc) {
        if(cl.hasOption(OPTIONS.url.getArg())) {
            jdbc.setURL(cl.getOptionValue(OPTIONS.url.getArg()));
        }
        if(cl.hasOption(OPTIONS.statement.getArg())) {
            jdbc.setSTATEMENT(cl.getOptionValue(OPTIONS.statement.getArg()));
        }
        if(cl.hasOption(OPTIONS.timeout.getArg())) {
            try {
                jdbc.setTIMEOUT(Integer.parseInt(cl.getOptionValue(OPTIONS.timeout.getArg())));
            } catch(NumberFormatException n) {
                printParserError(n);
                return false;
            }
        }
        if(cl.hasOption(OPTIONS.numsets.getArg())) {
            try {
                jdbc.setRESULTSETS(Integer.parseInt(cl.getOptionValue(OPTIONS.numsets.getArg())));
            } catch(NumberFormatException n) {
                printParserError(n);
                return false;
            }
        }
        if(cl.hasOption(OPTIONS.fetchsize.getArg())) {
            try {
                jdbc.setFETCHSIZE(Integer.parseInt(cl.getOptionValue(OPTIONS.fetchsize.getArg())));
            } catch(NumberFormatException n) {
                printParserError(n);
                return false;
            }
        }
        if(cl.hasOption(OPTIONS.dots.getArg())) {
            jdbc.setDOTS(true);
        }
        return true;
    }
    
    private static void printParserError(Exception e) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("ScrayJdbcAccess", setupCLIOptions(), true);
        System.err.println("ERROR:" + e.getLocalizedMessage());        
    }
    
    public static boolean parseCLIOptions(ScrayJdbcAccess jdbc, String[] args) {
        GnuParser parser = new GnuParser();
        HelpFormatter help = new HelpFormatter();
        Options options = setupCLIOptions();
        try {
            CommandLine cl = parser.parse(options, args);
            if(cl.hasOption(OPTIONS.help.getArg())) {
                help.printHelp("ScrayJdbcAccess", options, true);
            } else {
                return interpretCommandLine(cl, jdbc);
            }
        } catch(ParseException p) {
            printParserError(p);
        }
        return false;
    }
}
