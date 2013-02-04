/*
* Copyright 2010-2011 DIMA Research Group, TU Berlin
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package de.tuberlin.dima.myriad;

import java.io.File;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.Iterator;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;


/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class Frontend {
	
	private JSAP optionsParser = new JSAP();

	public Frontend() {
		initialize();
	}
	
	/**
	 * 
	 */
	private void initialize() {
		try {
			// scaling-factor
			FlaggedOption optScalingFactor = new FlaggedOption("scaling-factor")
		        .setShortFlag('s') 
		        .setLongFlag("scaling-factor")
		        .setStringParser(JSAP.DOUBLE_PARSER)
		        .setRequired(true)
		        .setDefault("1.0");
			optScalingFactor.setHelp("scaling factor (s=1 generates 1GB)");
			this.optionsParser.registerParameter(optScalingFactor);
			
			// dataset-id
			FlaggedOption optDatasetID = new FlaggedOption("dataset-id") 
		        .setShortFlag('m') 
		        .setLongFlag("dataset-id")
		        .setStringParser(JSAP.STRING_PARSER)
		        .setRequired(true) 
		        .setDefault("default-dataset");
			optDatasetID.setHelp("ID of the generated Myriad dataset");
			this.optionsParser.registerParameter(optDatasetID);
			
			// node-id
			FlaggedOption optNodeID = new FlaggedOption("nodeID")
				.setShortFlag('i') 
				.setLongFlag("node-id")
				.setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(true)
				.setDefault("0");
			optNodeID.setHelp("node ID (i.e. partition number) of the current generating node");
			this.optionsParser.registerParameter(optNodeID);
			
			// node-count
			FlaggedOption optNodeCount = new FlaggedOption("node-count")
		        .setShortFlag('N') 
		        .setLongFlag("node-count")
		        .setStringParser(JSAP.INTEGER_PARSER)
		        .setRequired(true)
		        .setDefault("1");
			optNodeCount.setHelp("total node count (i.e. total number of partitions)");
			this.optionsParser.registerParameter(optNodeCount);
			
			// output-base
			FlaggedOption optOutputBase = new FlaggedOption("output-base") 
		        .setShortFlag('o') 
		        .setLongFlag("output-base")
		        .setStringParser(JSAP.STRING_PARSER)
		        .setRequired(true)
		        .setDefault("/");
			optOutputBase.setHelp("base path for writing the output");
			this.optionsParser.registerParameter(optOutputBase);
			
			// execute-stages
			FlaggedOption optExecuteStages = new FlaggedOption("execute-stage") 
		        .setShortFlag('x') 
		        .setLongFlag("execute-stage")
		        .setStringParser(JSAP.STRING_PARSER)
		        .setRequired(true);
			optExecuteStages.setHelp("specify a specifc stage to be executed");
			this.optionsParser.registerParameter(optExecuteStages);
			
			// dgen-path
			UnflaggedOption optDGenPath = new UnflaggedOption("dgen-path") 
		        .setStringParser(JSAP.STRING_PARSER)
		        .setRequired(true);
			optDGenPath.setHelp("full path to the data generator executable");
			this.optionsParser.registerParameter(optDGenPath);
			
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
        
        // 
	}
	
	private void printErrors(PrintStream out, JSAPResult parsedOptions) {
        for (@SuppressWarnings("rawtypes") Iterator errs = parsedOptions.getErrorMessageIterator(); errs.hasNext();) {
            out.println("Error: " + errs.next());
        }
        out.println();
	}
	
	private void printUsage(PrintStream out) {
        out.println("Usage: java -jar " + getJar() + " [OPTIONS] <dgen-path>");
        out.println();
	}
	
	private void printHelp(PrintStream out) {
        out.println(this.optionsParser.getHelp());
	}

	public static void main(String[] args) {
		
		Frontend frontend = new Frontend();
		
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
			CodeSource codeSource = Frontend.class.getProtectionDomain().getCodeSource();
			File jarFile;
			jarFile = new File(codeSource.getLocation().toURI().getPath());
			return jarFile.getName();
		} catch (URISyntaxException e) {
			return "<unknown-jar>";
		}
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