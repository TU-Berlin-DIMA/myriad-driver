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
import org.apache.hadoop.fs.FileSystem;
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

import eu.stratosphere.myriad.driver.MyriadDriverJob;
import eu.stratosphere.myriad.driver.parameters.DriverJobParameters;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@SuppressWarnings("deprecation")
public class MyriadDriverHadoopJob extends Configured implements MyriadDriverJob {

	private DriverJobParameters parameters;

	public MyriadDriverHadoopJob(DriverJobParameters parameters) {
		super(new Configuration());
		this.parameters = parameters;
	}

	@Override
	public void run() throws IOException {
		JobClient.runJob(this.createJobConf());
	}

	@Override
	public void removeOutputPath() throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		Path outputPath = new Path(this.parameters.getJobOutputPath());

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
	}

	private JobConf createJobConf() {
		// create job
		JobConf conf = new JobConf(getConf());

		conf.setJarByClass(MyriadDriverHadoopJob.class);
		conf.setJobName(String.format("%s", this.parameters.getDGenName()));

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(IdentityMapper.class);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(MyriadInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// input format configuration
		MyriadInputFormat.setDriverJobParameters(conf, this.parameters);
		// output format configuration
		FileOutputFormat.setOutputPath(conf, new Path(this.parameters.getJobOutputPath()));

		return conf;
	}

	public static class IdentityMapper implements Mapper<NullWritable, Text, NullWritable, Text> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void map(NullWritable k, Text v, OutputCollector<NullWritable, Text> o, Reporter r) throws IOException {
			o.collect(k, v);
		}
	}
}
