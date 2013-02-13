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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import eu.stratosphere.myriad.driver.parameters.DriverJobParameters;
import eu.stratosphere.myriad.driver.parameters.SocketReaderParameters;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@SuppressWarnings("deprecation")
public class MyriadInputFormat implements InputFormat<NullWritable, Text> {
	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
		// get number of splits
		int nodeCount = MyriadInputFormat.getNodeCount(conf);

		// construct splits
		InputSplit[] splits = new InputSplit[nodeCount];
		for (int nodeID = 0; nodeID < nodeCount; nodeID++) {
			splits[nodeID] = new MyriadInputSplit(nodeID);
		}

		return splits;
	}

	@Override
	public RecordReader<NullWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new MyriadRecordReader((MyriadInputSplit) split, job);
	}

	public static void setDGenInstallDir(JobConf conf, String dgenNodePath) {
		conf.set("mapred.myriad.dgen.node.path", dgenNodePath);
	}

	public static String getDGenInstallDir(JobConf conf) {
		String nodePath = conf.get("mapred.myriad.dgen.node.path", "");
		if (nodePath == "") {
			throw new IllegalArgumentException("Bad `mapred.myriad.dgen.node.path` parameter value");
		}
		return nodePath;
	}

	public static void setScalingFactor(JobConf conf, float scalingFactor) {
		conf.setFloat("mapred.myriad.dgen.scaling.factor", scalingFactor);
	}

	public static float getScalingFactor(JobConf conf) {
		float scalingFactor = conf.getFloat("mapred.myriad.dgen.scaling.factor", -1);
		if (scalingFactor <= 0) {
			throw new IllegalArgumentException("Bad `mapred.myriad.dgen.scaling.factor` parameter value");
		}
		return scalingFactor;
	}

	public static void setNodeCount(JobConf conf, short nodeCount) {
		conf.setInt("mapred.myriad.dgen.node.count", nodeCount);
	}

	public static int getNodeCount(JobConf conf) {
		int nodeCount = conf.getInt("mapred.myriad.dgen.node.count", -1);
		if (nodeCount < 1) {
			throw new IllegalArgumentException("Bad `mapred.myriad.dgen.node.count` parameter value");
		}
		return nodeCount;
	}

	public static void setStage(JobConf conf, String stage) {
		conf.set("mapred.myriad.dgen.stage", stage);
	}

	public static String getStage(JobConf conf) {
		String stage = conf.get("mapred.myriad.dgen.stage", "");
		if (stage == "") {
			throw new IllegalArgumentException("Bad `mapred.myriad.dgen.stage` parameter value");
		}
		return stage;
	}

	public static void setOutputBase(JobConf conf, String outputBase) {
		conf.set("mapred.myriad.dgen.output.base", outputBase);
	}

	public static String getOutputBase(JobConf conf) {
		String nodePath = conf.get("mapred.myriad.dgen.output.base", "");
		if (nodePath == "") {
			throw new IllegalArgumentException("Bad `mapred.myriad.dgen.output.base` parameter value");
		}
		return nodePath;
	}

	public static void setDatasetID(JobConf conf, String datasetID) {
		conf.set("mapred.myriad.dgen.dataset.id", datasetID);
	}

	public static String getDatasetID(JobConf conf) {
		String nodePath = conf.get("mapred.myriad.dgen.dataset.id", "");
		if (nodePath == "") {
			throw new IllegalArgumentException("Bad `mapred.myriad.dgen.dataset.id` parameter value");
		}
		return nodePath;
	}

	/**
	 * @param parameters
	 */
	public static void setDriverJobParameters(JobConf conf, DriverJobParameters parameters) {
		MyriadInputFormat.setDGenInstallDir(conf, parameters.getDGenInstallDir().toString());
		MyriadInputFormat.setOutputBase(conf, parameters.getOutputBase().toString());
		MyriadInputFormat.setDatasetID(conf, parameters.getDatasetID());
		MyriadInputFormat.setStage(conf, parameters.getStage());
		MyriadInputFormat.setScalingFactor(conf, parameters.getScalingFactor());
		MyriadInputFormat.setNodeCount(conf, parameters.getNodeCount());
	}

	/**
	 * @param conf
	 * @param nodeID
	 * @return
	 */
	public static SocketReaderParameters getDriverJobParameters(JobConf conf, short nodeID) {
		File dgenInstallDir = new File(MyriadInputFormat.getDGenInstallDir(conf));
		File outputBase = new File(MyriadInputFormat.getOutputBase(conf));
		String datasetID = MyriadInputFormat.getDatasetID(conf);
		String stage = MyriadInputFormat.getStage(conf);
		float scalingFactor = MyriadInputFormat.getScalingFactor(conf);
		short nodeCount = (short) MyriadInputFormat.getNodeCount(conf);
		return new SocketReaderParameters(dgenInstallDir, outputBase, datasetID, stage, scalingFactor, nodeCount, nodeID);
	}
}