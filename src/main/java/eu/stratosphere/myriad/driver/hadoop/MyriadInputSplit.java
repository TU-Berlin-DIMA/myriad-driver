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
package eu.stratosphere.myriad.driver.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@SuppressWarnings("deprecation")
public class MyriadInputSplit implements InputSplit {

	private int nodeID;

	MyriadInputSplit() {
	}

	/**
	 * Initializes MyriadInputSplit.
	 *
	 * @param nodePath
	 * @param stage
	 * @param scalingFactor
	 * @param nodeCount
	 * @param nodeID
	 */
	public MyriadInputSplit(int nodeID) {
		this.nodeID = nodeID;
	}

	public int getNodeID() {
		return this.nodeID;
	}

	@Override
	public long getLength() throws IOException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[] { };
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.nodeID = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.nodeID);
	}
}