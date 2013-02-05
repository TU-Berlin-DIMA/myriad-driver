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

import com.martiansoftware.jsap.JSAPResult;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class DriverJobParameters {

	private final File dgenInstallDir;

	private final File dgenNodePath;

	private final File outputBase;

	private final String dgenName;

	private final String datasetID;

	private final String stage;

	private final double scalingFactor;

	private final short nodeCount;

	public DriverJobParameters(JSAPResult parsedOptions, String stage) throws DriverJobParametersException {
		this.dgenInstallDir = parsedOptions.getFile("dgen-install-dir").getAbsoluteFile();
		this.dgenName = this.dgenInstallDir.getName();
		this.dgenNodePath = new File(String.format("%s/bin/%s-node", this.dgenInstallDir, this.dgenName));
		this.outputBase = parsedOptions.getFile("output-base");
		this.datasetID = parsedOptions.getString("dataset-id");
		this.stage = stage;
		this.scalingFactor = parsedOptions.getDouble("scaling-factor");
		this.nodeCount = parsedOptions.getShort("node-count");

		validateParameters();
	}

	private void validateParameters() throws DriverJobParametersException {
		if (!this.dgenInstallDir.isDirectory()) {
			throw new DriverJobParametersException("Data generator install dir `" + this.dgenInstallDir
				+ "` does not exist.");
		}
		if (!this.dgenNodePath.isFile()) {
			throw new DriverJobParametersException("Data generator executable node `" + this.dgenNodePath
				+ "` does not exist.");
		}
		if (!this.outputBase.isAbsolute()) {
			throw new DriverJobParametersException("Base output path `" + this.outputBase + "` should be absolute.");
		}
	}

	public File getDgenInstallDir() {
		return this.dgenInstallDir;
	}

	public File getDGenNodePath() {
		return this.dgenNodePath;
	}

	public File getOutputBase() {
		return this.outputBase;
	}

	public String getDgenName() {
		return this.dgenName;
	}

	public String getDatasetID() {
		return this.datasetID;
	}

	public String getStage() {
		return this.stage;
	}

	public double getScalingFactor() {
		return this.scalingFactor;
	}

	public short getNodeCount() {
		return this.nodeCount;
	}

	public String getJobOutputPath() {
		return String.format("%s/%s/%s", this.outputBase, this.datasetID, this.stage);
	}
}
