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

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.myriad.driver.parameters.SocketReaderParameters;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class MyriadSocketReaderTest {

	private SocketReaderParameters socketReaderParameters;

	private MyriadSocketReader socketReader;

	@Before
	public void setUp() {
		File dgenInstallDir = new File("/home/alexander/etc/datagen/wordcount-gen");
		File outputBase = new File("/tmp");
		String datasetID = "wordcount-gen-sf0001";
		String stage = "token";
		float sf = 1.0f;
		short N = 1;
		short i = 0;

		this.socketReaderParameters = new SocketReaderParameters(dgenInstallDir, outputBase, datasetID, stage, sf, N, i);
	}

	@Test
	@Ignore("Ignore test with hardcoded external Myriad Data Generator dependency")
	public void testSocketReader() throws IOException {
		this.socketReader = new MyriadSocketReader(this.socketReaderParameters);

		String line;
		while ((line = this.socketReader.next()) != null) {
			System.out.println(line);
		}
	}
}
