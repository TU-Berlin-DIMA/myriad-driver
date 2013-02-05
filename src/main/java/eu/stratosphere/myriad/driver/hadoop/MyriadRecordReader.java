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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@SuppressWarnings("deprecation")
public class MyriadRecordReader implements RecordReader<NullWritable, Text> {

	private MyriadSocketReader socketReader;

	/**
	 * Initializes MyriadRecordReader.
	 * 
	 * @param split
	 * @param job
	 */
	public MyriadRecordReader(MyriadInputSplit split, JobConf job) {
		this.socketReader = new MyriadSocketReader(split, job);
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.RecordReader#createKey()
	 */
	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.RecordReader#createValue()
	 */
	@Override
	public Text createValue() {
		return new Text();
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.RecordReader#getPos()
	 */
	@Override
	public long getPos() throws IOException {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.RecordReader#getProgress()
	 */
	@Override
	public float getProgress() throws IOException {
		return this.socketReader.getProgress();
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean next(NullWritable key, Text value) throws IOException {
		return this.socketReader.next(value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.RecordReader#close()
	 */
	@Override
	public void close() throws IOException {
		this.socketReader.close();
	}
}