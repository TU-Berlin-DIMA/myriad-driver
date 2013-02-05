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
package eu.stratosphere.myriad.driver.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import eu.stratosphere.myriad.driver.parameters.SocketReaderParameters;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class MyriadSocketReader {

	private final String nodePath;

	private final String outputBase;

	private final String datasetID;

	private final String stage;

	private final double scalingFactor;

	private final int nodeCount;

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
	public MyriadSocketReader(SocketReaderParameters parameters) {
		// read input parameters from job config
		this.nodePath = parameters.getDGenNodePath().getAbsolutePath();
		this.outputBase = parameters.getOutputBase().getAbsolutePath();
		this.datasetID = parameters.getDatasetID();
		this.stage = parameters.getStage();
		this.scalingFactor = parameters.getScalingFactor();
		this.nodeCount = parameters.getNodeCount();
		this.nodeID = parameters.getNodeID();
		// compute derived parameters
		this.serverSocketPort = getOutputSocketPort();
		
		System.out.println("open server at input socket number");
		// open server at input socket number
		try {
			this.serverSocket = new ServerSocket(this.serverSocketPort);
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: could not listen on port: " + this.serverSocketPort + ".");
		}

		System.out.println("start data generator process: " + getDGenCommand());
		// start data generator process
		try {
			this.dgenProcess = Runtime.getRuntime().exec(getDGenCommand());
		} catch (IOException e1) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: failed to start data generator process.");
		}

		System.out.println("create client socket from socket server");
		// create client socket from socket server
		try {
			this.clientSocket = this.serverSocket.accept();
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Myriad Data Generator: failed to open receiver socket.");
		}

		System.out.println("create input reader for client socket");
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
	public String next() throws IOException {
		return this.inputReader.readLine();
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

	private int getOutputSocketPort() {
		MessageDigest m;
		try {
			m = MessageDigest.getInstance("MD5");
			m.update(String.format("stage-%s_part-%04d", this.stage, this.nodeID).getBytes("US-ASCII"));
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
		sb.append(" -s").append(this.scalingFactor);
		sb.append(" -i").append(this.nodeID);
		sb.append(" -N").append(this.nodeCount);
		sb.append(" -m").append(this.datasetID);
		sb.append(" -x").append(this.stage);
		sb.append(" -o").append(this.outputBase);
		sb.append(" -t").append("socket[" + getOutputSocketPort() + "]");
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
