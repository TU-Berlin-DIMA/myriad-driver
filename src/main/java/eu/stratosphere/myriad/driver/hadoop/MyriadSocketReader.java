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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@SuppressWarnings("deprecation")
public class MyriadSocketReader {

	private final String nodePath;

	private final double scalingFactor;

	private final int nodeCount;

	private final String stage;

	private final String outputBase;

	private final String datasetID;

	private final int nodeID;
	
	private final int serverSocketPort;

	private final ServerSocket serverSocket;

	private final Socket clientSocket;

	private final BufferedReader inputReader;

	private final Process dgenProcess;

	private Thread dgenReaderThread;

	/**
	 * Initializes MyriadSocketReader.
	 * 
	 * @param split
	 */
	public MyriadSocketReader(MyriadInputSplit split, JobConf conf) {

		// read input parameters from job config
		this.nodePath = MyriadInputFormat.getDGenNodePath(conf);
		this.scalingFactor = MyriadInputFormat.getScalingFactor(conf);
		this.nodeCount = MyriadInputFormat.getNodeCount(conf);
		this.stage = MyriadInputFormat.getStage(conf);
		this.outputBase = MyriadInputFormat.getOutputBase(conf);
		this.datasetID = MyriadInputFormat.getDatasetID(conf);
		// read input parameters from split
		this.nodeID = split.getNodeID();
		// compute derived parameters
		this.serverSocketPort = getOutputSocketPort(getDGenOutputPath());
		
		// open server at input socket number
		try {
			this.serverSocket = new ServerSocket(this.serverSocketPort);
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: could not listen on port: " + this.serverSocketPort + ".");
		}

		// start data generator process
		try {
			this.dgenProcess = Runtime.getRuntime().exec(getDGenCommand());
		} catch (IOException e1) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: failed to start data generator process.");
		}

		// create client socket from socket server
		try {
			this.clientSocket = this.serverSocket.accept();
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: failed to open receiver socket.");
		}

		// create input reader for client socket
		try {
			this.inputReader = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: failed to open input stream.");
		}

		// create reader thread for the process (ignores the stdout)
		this.dgenReaderThread = new Thread(new MyriadDGenRunner());
		this.dgenReaderThread.start();
	}

	/**
	 * @return
	 */
	public float getProgress() {
		return 0;
	}

	/**
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean next(Text value) throws IOException {
		String inputLine = this.inputReader.readLine();

		if (inputLine == null) {
			return false;
		}

		value.set(inputLine);
		return true;
	}

	/**
	 * 
	 */
	public void close() {
	}

	/**
	 * 
	 */
	private void cleanup() {
		try {
			// close input stream
			if (this.inputReader != null) {
				this.inputReader.close();
			}
			// close client socket
			if (this.clientSocket != null) {
				this.clientSocket.close();
			}
			// close dgen process
			if (this.dgenProcess != null) {
				this.dgenProcess.destroy();
			}
			// close server socket
			if (this.serverSocket != null) {
				this.serverSocket.close();
			}
			// wait for reader thread to close
			if (this.dgenReaderThread != null) {
				this.dgenReaderThread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return
	 */
	private String getDGenOutputPath() {
		return String.format("%s/%s/node%03d/%s", this.outputBase, this.datasetID, this.nodeID, this.stage);
	}

	private int getOutputSocketPort(String outputPath) {
		MessageDigest m;
		try {
			m = MessageDigest.getInstance("MD5");
			m.update(outputPath.getBytes("US-ASCII"));
			byte[] hashValue = m.digest();
			long hashSuffix = ((0x000000FF & (long) hashValue[hashValue.length - 4]) << 48)
				| ((0x000000FF & (long) hashValue[hashValue.length - 3]) << 32)
				| ((0x000000FF & (long) hashValue[hashValue.length - 2]) << 16)
				| ((0x000000FF & (long) hashValue[hashValue.length - 1]) << 0);

			return (int) (42100 + ((hashSuffix % 900 < 0) ? (1000 - (hashSuffix % 900)) : (hashSuffix % 900)));
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return
	 */
	private String getDGenCommand() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.nodePath).append(" ");
		sb.append(" -s ").append(this.scalingFactor);
		sb.append(" -i ").append(this.nodeID);
		sb.append(" -N ").append(this.nodeCount);
		sb.append(" -m ").append(this.datasetID);
		sb.append(" -x ").append(this.stage);
		sb.append(" -o ").append(this.outputBase);
		sb.append(" -t ").append("socket");
		return sb.toString();
	}

	private class MyriadDGenRunner implements Runnable {

		@Override
		public void run() {
			try {
				InputStream in = MyriadSocketReader.this.dgenProcess.getInputStream();
				BufferedReader input = new BufferedReader(new InputStreamReader(in));
				while (input.readLine() != null) {
					// ignore the output
				}
				input.close();
			} catch (IOException e) {
				// do nothing
			}
		}
	}
}
