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
import java.util.Iterator;

import eu.stratosphere.myriad.driver.ParsedOptions;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class DriverJobParametersFamily implements Iterable<DriverJobParameters> {

	private ParsedOptions parsedOptions;

	/**
	 * Initializes DriverJobParametersIterator.
	 * 
	 * @param parsedOptions
	 */
	public DriverJobParametersFamily(ParsedOptions parsedOptions) {
		this.parsedOptions = parsedOptions;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<DriverJobParameters> iterator() {
		return new DriverJobParametersIterator(this.parsedOptions);
	}

	private static class DriverJobParametersIterator implements Iterator<DriverJobParameters> {

		private final File dgenInstallDir;

		private final File outputBase;

		private final String datasetID;

		private final float scalingFactor;

		private final short nodeCount;

		private final String[] stages;

		private int currentStage;

		/**
		 * Initializes DriverJobParametersIterator.
		 * 
		 * @param parsedOptions
		 */
		public DriverJobParametersIterator(ParsedOptions parsedOptions) {
			this.dgenInstallDir = parsedOptions.getFile("dgen-install-dir").getAbsoluteFile();
			this.outputBase = parsedOptions.getFile("output-base");
			this.datasetID = parsedOptions.getString("dataset-id");
			this.scalingFactor = parsedOptions.getFloat("scaling-factor");
			this.nodeCount = parsedOptions.getShort("node-count");
			this.stages = parsedOptions.getStringArray("execute-stage");
			this.currentStage = 0;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return this.currentStage < this.stages.length;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public DriverJobParameters next() {
			return new DriverJobParameters(this.dgenInstallDir, this.outputBase, this.datasetID,
				this.stages[this.currentStage++], this.scalingFactor, this.nodeCount);
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#remove()
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
}
