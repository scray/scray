package scray.client.test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ScrayJdbcAccessParser {
	private static enum OPTIONS {
		help("h"), query("q"), numsets("n"), url("u"), timeout("o"), fetchsize(
				"f"), data("d"), stress("s");
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
		Option url = new Option(
				OPTIONS.url.getArg(),
				"url",
				true,
				"database-url in scray-url format, i.e. jdbc:scray:<stateless|stateful>://<host>:<port>/<dbsystem>/<dbid>/<queryspace>");
		url.setArgName("URL");
		url.setRequired(false);
		options.addOption(url);
		Option numsets = new Option(OPTIONS.numsets.getArg(), "numsets", true,
				"number of result sets to fetch, -1 for all (default) -> multiply with f");
		numsets.setArgName("NUMBER");
		numsets.setRequired(false);
		options.addOption(numsets);
		Option query = new Option(OPTIONS.query.getArg(), "query", true,
				"the query statement.");
		query.setArgName("QUERY");
		query.setRequired(false);
		options.addOption(query);
		Option timeout = new Option(OPTIONS.timeout.getArg(), "timeout", true,
				"timeout, default = 60");
		timeout.setArgName("SECONDS");
		timeout.setRequired(false);
		options.addOption(timeout);
		Option fetchsize = new Option(OPTIONS.fetchsize.getArg(), "fetchsize",
				true, "fetch size for each result set, default = 50");
		fetchsize.setArgName("SIZE");
		fetchsize.setRequired(false);
		options.addOption(fetchsize);
		Option help = new Option(OPTIONS.help.getArg(), "help", false,
				"print usage information");
		help.setRequired(false);
		options.addOption(help);
		Option data = new Option(OPTIONS.data.getArg(), "data", false,
				"Print actual datasets. Default is to print just a dot representing 10000 results.");
		data.setRequired(false);
		options.addOption(data);
		Option stress = new Option(
				OPTIONS.stress.getArg(),
				"stress",
				true,
				"stresstest with continuous parallel queries (10 in parallel), default = 1000, -1 for infinite.");
		stress.setArgName("QUERIES");
		stress.setRequired(false);
		options.addOption(stress);
		return options;
	}

	private static boolean interpretCommandLine(CommandLine cl,
			ScrayJdbcAccess.AccessRequestOptions opts) {
		if (cl.hasOption(OPTIONS.url.getArg())) {
			opts.url = cl.getOptionValue(OPTIONS.url.getArg());
		}
		if (cl.hasOption(OPTIONS.query.getArg())) {
			opts.query = cl.getOptionValue(OPTIONS.query.getArg());
		}
		if (cl.hasOption(OPTIONS.timeout.getArg())) {
			try {
				opts.timeout = Integer.parseInt(cl
						.getOptionValue(OPTIONS.timeout.getArg()));
			} catch (NumberFormatException n) {
				printParserError(n);
				return false;
			}
		}
		if (cl.hasOption(OPTIONS.numsets.getArg())) {
			try {
				opts.resultsets = Integer.parseInt(cl
						.getOptionValue(OPTIONS.numsets.getArg()));
			} catch (NumberFormatException n) {
				printParserError(n);
				return false;
			}
		}
		if (cl.hasOption(OPTIONS.fetchsize.getArg())) {
			try {
				opts.fetchsize = Integer.parseInt(cl
						.getOptionValue(OPTIONS.fetchsize.getArg()));
			} catch (NumberFormatException n) {
				printParserError(n);
				return false;
			}
		}
		if (cl.hasOption(OPTIONS.stress.getArg())) {
			try {
				opts.stress = true;
				opts.stressCount = Integer.parseInt(cl
						.getOptionValue(OPTIONS.stress.getArg()));
			} catch (NumberFormatException n) {
				printParserError(n);
				return false;
			}
		}
		if (cl.hasOption(OPTIONS.data.getArg())) {
			opts.dots = false;
		}
		return true;
	}

	private static void printParserError(Exception e) {
		HelpFormatter help = new HelpFormatter();
		help.printHelp("ScrayJdbcAccess", setupCLIOptions(), true);
		System.err.println("ERROR:" + e.getLocalizedMessage());
	}

	public static boolean parseCLIOptions(
			ScrayJdbcAccess.AccessRequestOptions opts, String[] args) {
		GnuParser parser = new GnuParser();
		HelpFormatter help = new HelpFormatter();
		Options options = setupCLIOptions();
		try {
			CommandLine cl = parser.parse(options, args);
			if (cl.hasOption(OPTIONS.help.getArg())) {
				help.printHelp("ScrayJdbcAccess", options, true);
			} else {
				return interpretCommandLine(cl, opts);
			}
		} catch (ParseException p) {
			printParserError(p);
		}
		return false;
	}
}
