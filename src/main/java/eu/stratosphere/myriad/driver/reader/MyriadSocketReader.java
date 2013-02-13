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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import eu.stratosphere.myriad.driver.parameters.SocketReaderParameters;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class MyriadSocketReader {
	
	private static int BUFFER_SIZE = 4194304; // 4MB buffer

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

	private final int heartBeatServerPort;

	private final Server heartBeatServer;

	private final BufferedReader inputReader;

	private float dgenProgress = 0.0f;

	private final Process dgenProcess;

	private final Thread dgenReaderThread;

	/**
	 * Initializes MyriadSocketReader.
	 * 
	 * @param parameters
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

		// open SocketReader server at input socket number
		try {
			this.serverSocket = new ServerSocket(0);
			this.serverSocketPort = this.serverSocket.getLocalPort();
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Could not open reader server socket.");
		}

		// open heartbeat HTTP server
		try {
			org.mortbay.log.Log.setLog(null); // disable the jetty log
			this.heartBeatServer = new Server(0);
			this.heartBeatServer.setHandler(new HeartBeatHandler());
			this.heartBeatServer.start();
			this.heartBeatServerPort = this.heartBeatServer.getConnectors()[0].getLocalPort();
		} catch (Exception e) {
			cleanup();
			throw new RuntimeException("Could not open heart beat server socket.");
		}

		// start data generator process
		try {
			this.dgenProcess = Runtime.getRuntime().exec(getDGenCommand());
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Failed to start data generator process.");
		}

		// create client socket from socket server
		try {
			this.clientSocket = this.serverSocket.accept();
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Failed to open receiver socket.");
		}

		// create input reader for client socket
		try {
			this.inputReader = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()),
				MyriadSocketReader.BUFFER_SIZE);
		} catch (IOException e) {
			cleanup();
			throw new RuntimeException("Failed to open input stream.");
		}

		// create reader thread for the process (ignores the stdout)
		this.dgenReaderThread = new Thread(new MyriadDGenRunner());
		this.dgenReaderThread.start();
	}

	/**
	 * @return
	 */
	public float getProgress() {
		synchronized (this.dgenProcess) {
			return this.dgenProgress;
		}
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
		cleanup();
	}

	/**
	 * 
	 */
	private void cleanup() {
		try {
			// wait for reader thread to close
			if (this.dgenReaderThread != null) {
				this.dgenReaderThread.join(3000); // give the dgen process three seconds to finish
			}
			// close dgen process
			if (this.dgenProcess != null) {
				try {
					this.dgenProcess.exitValue(); // do nothing if already terminated
				} catch (IllegalThreadStateException e) {
					this.dgenProcess.waitFor();
					// this.dgenProcess.destroy(); // else brute-force terminate
				}
			}
			// close input stream
			if (this.inputReader != null) {
				this.inputReader.close();
			}
			// close client socket
			if (this.clientSocket != null) {
				this.clientSocket.close();
			}
			// close heartbeat server
			if (this.heartBeatServer != null) {
				this.heartBeatServer.setGracefulShutdown(1000); // stop after 1 second
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

	@SuppressWarnings("unused")
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

			return (int) (43000 + ((hashSuffix % 1000 < 0) ? (1000 - (hashSuffix % 1000)) : (hashSuffix % 1000)));
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
		sb.append("time --output=/tmp/").append(this.datasetID).append(".").append(this.nodeID).append(".txt ");
		sb.append(this.nodePath).append(" ");
		sb.append(" -s").append(this.scalingFactor);
		sb.append(" -i").append(this.nodeID);
		sb.append(" -N").append(this.nodeCount);
		sb.append(" -m").append(this.datasetID);
		sb.append(" -x").append(this.stage);
		sb.append(" -o").append(this.outputBase);
		sb.append(" -t").append("socket[" + this.serverSocketPort + "]");
		sb.append(" -H").append("localhost");
		sb.append(" -P").append(this.heartBeatServerPort);
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

	private class HeartBeatHandler extends AbstractHandler {

		/*
		 * (non-Javadoc)
		 * @see org.mortbay.jetty.Handler#handle(java.lang.String, javax.servlet.http.HttpServletRequest,
		 * javax.servlet.http.HttpServletResponse, int)
		 */
		@Override
		public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
				throws IOException, ServletException {
			if ("HEAD".equals(request.getMethod())) {
				float progress;
				try {
					progress = Float.parseFloat(request.getParameter("progress"));
				} catch (NumberFormatException e) {
					progress = -1;
				}

				if (progress != -1) {
					synchronized (MyriadSocketReader.this.dgenProcess) {
						MyriadSocketReader.this.dgenProgress = progress;
					}
					System.out.println("Progress is now " + MyriadSocketReader.this.dgenProgress);
				}

				// write the response
				response.setStatus(HttpServletResponse.SC_OK);
				if (request instanceof Request) {
					((Request) request).setHandled(true);
				}
			}
		}
	}
}
