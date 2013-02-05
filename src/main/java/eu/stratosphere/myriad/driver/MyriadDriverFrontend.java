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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.util.Iterator;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.stringparsers.DoubleStringParser;
import com.martiansoftware.jsap.stringparsers.FileStringParser;
import com.martiansoftware.jsap.stringparsers.ShortStringParser;
import com.martiansoftware.jsap.stringparsers.StringStringParser;

import eu.stratosphere.myriad.driver.hadoop.MyriadDriverJob;
import eu.stratosphere.myriad.driver.parameters.DriverJobParameters;
import eu.stratosphere.myriad.driver.parameters.DriverJobParametersException;
import eu.stratosphere.myriad.driver.parameters.DriverJobParametersFamily;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class MyriadDriverFrontend {

	private JSAP optionsParser = new JSAP();

	public MyriadDriverFrontend() {
		initialize();
	}

	/**
	 * 
	 */
	private void initialize() {
		try {
			// dgen-install-dir
			UnflaggedOption optDGenInstallDir = new UnflaggedOption("dgen-install-dir");
			optDGenInstallDir.setStringParser(FileStringParser.getParser());
			optDGenInstallDir.setRequired(true);
			optDGenInstallDir.setHelp("absolute data generator installation directory");
			this.optionsParser.registerParameter(optDGenInstallDir);

			// scaling-factor
			FlaggedOption optScalingFactor = new FlaggedOption("scaling-factor");
			optScalingFactor.setShortFlag('s');
			optScalingFactor.setLongFlag("scaling-factor");
			optScalingFactor.setStringParser(DoubleStringParser.getParser());
			optScalingFactor.setRequired(true);
			optScalingFactor.setDefault("1.0");
			optScalingFactor.setHelp("scaling factor (s=1 generates 1GB)");
			this.optionsParser.registerParameter(optScalingFactor);

			// dataset-id
			FlaggedOption optDatasetID = new FlaggedOption("dataset-id");
			optDatasetID.setShortFlag('m');
			optDatasetID.setLongFlag("dataset-id");
			optDatasetID.setStringParser(StringStringParser.getParser());
			optDatasetID.setRequired(true);
			optDatasetID.setDefault("default-dataset");
			optDatasetID.setHelp("ID of the generated Myriad dataset");
			this.optionsParser.registerParameter(optDatasetID);

			// node-count
			FlaggedOption optNodeCount = new FlaggedOption("node-count");
			optNodeCount.setShortFlag('N');
			optNodeCount.setLongFlag("node-count");
			optNodeCount.setStringParser(ShortStringParser.getParser());
			optNodeCount.setRequired(true);
			optNodeCount.setDefault("1");
			optNodeCount.setHelp("degree of parallelism (i.e. total number of partitions)");
			this.optionsParser.registerParameter(optNodeCount);

			// output-base
			FlaggedOption optOutputBase = new FlaggedOption("output-base");
			optOutputBase.setShortFlag('o');
			optOutputBase.setLongFlag("output-base");
			optOutputBase.setStringParser(FileStringParser.getParser());
			optOutputBase.setRequired(true);
			optOutputBase.setDefault("/tmp");
			optOutputBase.setHelp("base path for writing the output");
			this.optionsParser.registerParameter(optOutputBase);

			// execute-stages
			FlaggedOption optExecuteStages = new FlaggedOption("execute-stage");
			optExecuteStages.setShortFlag('x');
			optExecuteStages.setLongFlag("execute-stage");
			optExecuteStages.setStringParser(StringStringParser.getParser());
			optExecuteStages.setRequired(true);
			optExecuteStages.setList(true);
			optExecuteStages.setListSeparator(',');
			optExecuteStages.setHelp("specify stages to be executed");
			this.optionsParser.registerParameter(optExecuteStages);

		} catch (JSAPException e) {
			System.err.println("Could not construct JSAP options: " + e.getMessage());
			System.exit(1);
		}
	}

	/**
	 * @param args
	 */
	private void process(String[] args) throws ParseOptionsException {
		// parse options
		JSAPResult parsedOptions = this.optionsParser.parse(args);

		// throw exception on parse failure
		if (!parsedOptions.success()) {
			throw new ParseOptionsException(parsedOptions);
		}

		try {
			// execute driver job for each 'execute-stage' parameter
			for (DriverJobParameters parameters : new DriverJobParametersFamily(parsedOptions)) {
				System.out.println("Running Myriad Data Generator for stage `" + parameters.getStage() + "`");
				// run driver job
				MyriadDriverJob myriadDriverJob = new MyriadDriverJob(parameters);
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

	private void printErrors(PrintStream out, JSAPResult parsedOptions) {
		@SuppressWarnings("rawtypes")
		Iterator errs = parsedOptions.getErrorMessageIterator();
		while (errs.hasNext()) {
			out.println("Error: " + errs.next());
		}
		out.println();
	}

	private void printUsage(PrintStream out) {
		out.println("Usage: (hadoop|pact-client) -jar " + getJar() + " <dgen-install-dir> [OPTIONS]");
		out.println();
	}

	private void printHelp(PrintStream out) {
		out.println(this.optionsParser.getHelp());
	}

	public static void main(String[] args) {
		
		MyriadDriverFrontend frontend = new MyriadDriverFrontend();

		try {
			frontend.process(args);
		} catch (ParseOptionsException e) {
			frontend.printErrors(System.err, e.getParsedOptions());
			frontend.printUsage(System.err);
			frontend.printHelp(System.err);
			System.exit(1);
		}
	}

	public static String getJar() {
		try {
			CodeSource codeSource = MyriadDriverFrontend.class.getProtectionDomain().getCodeSource();
			File jarFile;
			jarFile = new File(codeSource.getLocation().toURI().getPath());
			return jarFile.getName();
		} catch (URISyntaxException e) {
			return "<unknown-jar>";
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

	public static class ParseOptionsException extends Exception {

		private static final long serialVersionUID = -250326135439737607L;

		private final JSAPResult parsedOptions;

		public ParseOptionsException(JSAPResult parsedOptions) {
			this.parsedOptions = parsedOptions;
		}

		public JSAPResult getParsedOptions() {
			return this.parsedOptions;
		}
	}
}
