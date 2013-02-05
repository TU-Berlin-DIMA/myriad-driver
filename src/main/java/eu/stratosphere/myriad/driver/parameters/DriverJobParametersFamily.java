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

import java.util.Iterator;

import com.martiansoftware.jsap.JSAPResult;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class DriverJobParametersFamily implements Iterable<DriverJobParameters> {

	private JSAPResult parsedOptions;

	/**
	 * Initializes DriverJobParametersIterator.
	 * 
	 * @param parsedOptions
	 */
	public DriverJobParametersFamily(JSAPResult parsedOptions) {
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

		private final JSAPResult parsedOptions;

		private final String[] stages;
		
		private int currentStage;

		/**
		 * Initializes DriverJobParametersIterator.
		 * 
		 * @param parsedOptions
		 */
		public DriverJobParametersIterator(JSAPResult parsedOptions) {
			this.parsedOptions = parsedOptions;
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
			return new DriverJobParameters(this.parsedOptions, this.stages[this.currentStage++]);
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