/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.myriad.driver;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import eu.stratosphere.myriad.driver.hadoop.MyriadDriverHadoopJob;
import eu.stratosphere.myriad.driver.parameters.DriverJobParameters;
import eu.stratosphere.myriad.driver.parameters.DriverJobParametersException;
import eu.stratosphere.myriad.driver.parameters.DriverJobParametersFamily;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class MyriadDriverFrontend {

	private final Options options;

	public MyriadDriverFrontend() {
		// dgen-install-dir
		this.options = new Options();

		// scaling-factor
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("double");
		OptionBuilder.withDescription("scaling factor (s=1 generates 1GB)");
		OptionBuilder.withLongOpt("scaling-factor");
		this.options.addOption(OptionBuilder.create('s'));

		// dataset-id
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("string");
		OptionBuilder.withDescription("ID of the generated Myriad dataset");
		OptionBuilder.withLongOpt("dataset-id");
		this.options.addOption(OptionBuilder.create('m'));

		// node-count
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("int");
		OptionBuilder.withDescription("degree of parallelism (i.e. total number of partitions)");
		OptionBuilder.withArgName("node-count");
		this.options.addOption(OptionBuilder.create('N'));

		// output-base
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("path");
		OptionBuilder.withDescription("base path for writing the output");
		OptionBuilder.withLongOpt("output-base");
		this.options.addOption(OptionBuilder.create('o'));

		// execute-stages
		OptionBuilder.hasArgs();
		OptionBuilder.withArgName("stagename");
		OptionBuilder.withDescription("specify specific stages to be executed");
		OptionBuilder.withLongOpt("execute-stage");
		this.options.addOption(OptionBuilder.create('x'));
	}

	/**
	 * @param args
	 */
	private void process(String[] args) throws ParsedOptionsException, ParseException {
		// parse options
		ParsedOptions parsedOptions = this.parseOptions(args);

		// throw exception on parse failure
		if (!parsedOptions.success()) {
			throw new ParsedOptionsException(parsedOptions);
		}

		try {
			// execute a driver job for each 'execute-stage' parameter
			for (DriverJobParameters p : new DriverJobParametersFamily(parsedOptions)) {
				System.out.println(String.format("Running %s for stage `%s`", p.getDGenName(), p.getStage()));
				// run driver job
				MyriadDriverJob myriadDriverJob = driverJobFactory(p);
				myriadDriverJob.removeOutputPath();
				myriadDriverJob.run();
			}
		} catch (DriverJobParametersException e) {
			System.out.println("Exception in Hadoop MapReduce job parameters: " + e.getMessage());
			e.printStackTrace(System.err);
		} catch (IOException e) {
			System.out.println("Exception while running Hadoop MapReduce job: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}

	/**
	 * @param args
	 * @return
	 */
	private ParsedOptions parseOptions(String[] args) throws ParseException {
		ParsedOptions parsedOptions = new ParsedOptions();

		if (args.length < 1) {
			throw new ParseException("Missing dgen-install-dir argument");
		}

		parsedOptions.setFile("dgen-install-dir", new File(args[0]));

		// parse the command line arguments
		CommandLineParser parser = new PosixParser();
		CommandLine line = parser.parse(this.options, Arrays.copyOfRange(args, 1, args.length));

		if (line.hasOption('x')) {
			parsedOptions.setStringArray("execute-stage", line.getOptionValues('x'));
		} else {
			parsedOptions.setErrorMessage("execute-stage",
				"You should provide at least one data generator stage to be executed");
		}

		try {
			parsedOptions.setFloat("scaling-factor", Float.parseFloat(line.getOptionValue('s', "1.0")));
		} catch (NumberFormatException e) {
			parsedOptions.setErrorMessage("scaling-factor", e.getMessage());
		}

		try {
			parsedOptions.setShort("node-count", Short.parseShort(line.getOptionValue('N', "1")));
		} catch (NumberFormatException e) {
			parsedOptions.setErrorMessage("node-count", e.getMessage());
		}

		parsedOptions.setString("dataset-id", line.getOptionValue('m', "default-dataset"));
		parsedOptions.setFile("output-base", new File(line.getOptionValue('o', "/tmp")));

		return parsedOptions;
	}

	/**
	 * Factory method.
	 * 
	 * @param parameters
	 * @return
	 */
	private MyriadDriverJob driverJobFactory(DriverJobParameters parameters) {
		return new MyriadDriverHadoopJob(parameters);
	}

	private void printErrors(PrintStream out, ParsedOptions parsedOptions) {
		for (String message : parsedOptions.getErrorMessages()) {
			out.println("Error: " + message);
		}
		out.println();
	}

	private String getUsage() {
		return "(hadoop|pact-client) jar myriad-driver-jobs.jar <dgen-dir> [OPTIONS]";
	}

	private void printHelp(PrintStream out) {
		PrintWriter pw = new PrintWriter(out);
		int width = 80;
		int leftPad = 0;
		int descPad = 2;

		HelpFormatter formatter = new HelpFormatter();
		formatter.setSyntaxPrefix("Usage: ");
		formatter.printHelp(pw, width, getUsage(), "\nAvailable Options:\n \n", this.options, leftPad, descPad, null, false);

		pw.flush();
	}

	public static void main(String[] args) {
		MyriadDriverFrontend frontend = new MyriadDriverFrontend();

		try {
			frontend.process(args);
		} catch (ParseException e) {
			System.err.println("Could not parse command line string");
			System.err.println();
			frontend.printHelp(System.err);
			System.exit(1);
		} catch (ParsedOptionsException e) {
			frontend.printErrors(System.err, e.getParsedOptions());
			frontend.printHelp(System.err);
			System.exit(1);
		}
	}

	public static String getClasspathString() {
		StringBuffer classpath = new StringBuffer();
		ClassLoader applicationClassLoader = MyriadDriverFrontend.class.getClassLoader();
		if (applicationClassLoader == null) {
			applicationClassLoader = ClassLoader.getSystemClassLoader();
		}
		URL[] urls = ((URLClassLoader) applicationClassLoader).getURLs();
		for (int i = 0; i < urls.length; i++) {
			classpath.append(urls[i].getFile()).append("\r\n");
		}

		return classpath.toString();
	}
}
