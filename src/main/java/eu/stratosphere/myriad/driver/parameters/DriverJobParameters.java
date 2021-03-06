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
package eu.stratosphere.myriad.driver.parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class DriverJobParameters {

	private final File dgenInstallDir;

	private final File dgenNodePath;

	private final File dgenNodePropertiesPath;

	private final File outputBase;

	private final String dgenName;

	private final String datasetID;

	private final String stage;

	private final float scalingFactor;

	private final short nodeCount;

	private final Properties dgenNodeProperties;

	public DriverJobParameters(File dgenInstallDir, File outputBase, String datasetID, String stage,
			float scalingFactor, short nodeCount) throws DriverJobParametersException {
		this.dgenInstallDir = dgenInstallDir;
		this.dgenName = this.dgenInstallDir.getName();
		this.dgenNodePath = new File(String.format("%s/bin/%s-node", this.dgenInstallDir, this.dgenName));
		this.dgenNodePropertiesPath = new File(String.format("%s/config/%s-node.properties", this.dgenInstallDir,
			this.dgenName));
		this.outputBase = outputBase;
		this.datasetID = datasetID;
		this.stage = stage;
		this.scalingFactor = scalingFactor;
		this.nodeCount = nodeCount;

		validateParameters();

		this.dgenNodeProperties = loadDGenNodeProperties(this.dgenNodePropertiesPath);
	}

	private void validateParameters() throws DriverJobParametersException {
		if (!this.dgenInstallDir.isDirectory()) {
			throw new DriverJobParametersException("Data generator install dir `" + this.dgenInstallDir
				+ "` does not exist.");
		}
		if (!this.dgenNodePropertiesPath.isFile()) {
			throw new DriverJobParametersException("Data genenerator properties file `" + this.dgenNodePath
				+ "` does not exist.");
		}
		if (!this.dgenNodePath.isFile()) {
			throw new DriverJobParametersException("Data genenerator executable `" + this.dgenNodePath
				+ "` does not exist.");
		}
		if (!this.outputBase.isAbsolute()) {
			throw new DriverJobParametersException("Base output path `" + this.outputBase + "` should be absolute.");
		}
		if (!this.outputBase.isAbsolute()) {
			throw new DriverJobParametersException("Base output path `" + this.outputBase + "` should be absolute.");
		}
	}

	public File getDGenInstallDir() {
		return this.dgenInstallDir;
	}

	public File getDGenNodePath() {
		return this.dgenNodePath;
	}

	public File getOutputBase() {
		return this.outputBase;
	}

	public String getDGenName() {
		return this.dgenName;
	}

	public String getDatasetID() {
		return this.datasetID;
	}

	public String getStage() {
		return this.stage;
	}

	public float getScalingFactor() {
		return this.scalingFactor;
	}

	public short getNodeCount() {
		return this.nodeCount;
	}

	public String getJobOutputPath() {
		return String.format("%s/%s/%s", this.outputBase, this.datasetID, getOutputFile(this.stage));
	}
	
	private String getOutputFile(String stage) {
		return this.dgenNodeProperties.getProperty(String.format("generator.%s.output-file", stage), stage);
	}

	private static Properties loadDGenNodeProperties(File propertiesPath) {
		Properties properties = new Properties();
		try {
			FileInputStream input = new FileInputStream(propertiesPath);
			properties.load(input);
			input.close();
		} catch (IOException e) {
			throw new DriverJobParametersException("Could not load data generator node properties: " + e.getMessage());
		}
		return properties;
	}
}
