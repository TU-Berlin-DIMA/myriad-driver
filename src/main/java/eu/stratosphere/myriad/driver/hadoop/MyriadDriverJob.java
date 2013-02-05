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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import eu.stratosphere.myriad.driver.parameters.DriverJobParameters;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@SuppressWarnings("deprecation")
public class MyriadDriverJob extends Configured {

	private DriverJobParameters parameters;

	public MyriadDriverJob(DriverJobParameters parameters) {
		super(new Configuration());
		this.parameters = parameters;
	}

	public void run() throws IOException {
		JobClient.runJob(this.createJobConf());
	}

	public JobConf createJobConf() {
		// create job
		JobConf jobConf = new JobConf(getConf());

		jobConf.setJarByClass(MyriadDriverJob.class);
		jobConf.setJobName(String.format("%s", this.parameters.getDgenName()));

		jobConf.setOutputKeyClass(NullWritable.class);
		jobConf.setOutputValueClass(Text.class);

		jobConf.setMapperClass(IdentityMapper.class);

		jobConf.setInputFormat(MyriadInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		
		// input format configuration
		MyriadInputFormat.setDGenNodePath(jobConf, this.parameters.getDGenNodePath().toString());
		MyriadInputFormat.setStage(jobConf, this.parameters.getStage());
		MyriadInputFormat.setScalingFactor(jobConf, this.parameters.getScalingFactor());
		MyriadInputFormat.setNodeCount(jobConf, this.parameters.getNodeCount());
		MyriadInputFormat.setOutputBase(jobConf, this.parameters.getOutputBase().toString());
		MyriadInputFormat.setDatasetID(jobConf, this.parameters.getDatasetID());
		// output format configuration
		System.out.println(this.parameters.getJobOutputPath());
		FileOutputFormat.setOutputPath(jobConf, new Path(this.parameters.getJobOutputPath()));

		return jobConf;
	}

	public static class IdentityMapper implements Mapper<NullWritable, Text, NullWritable, Text> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void map(NullWritable key, Text val, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
			output.collect(key, val);
		}
	}
}
