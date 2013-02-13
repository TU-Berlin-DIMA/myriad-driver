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
package eu.stratosphere.myriad.driver;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
public class ParsedOptions {

	private final HashMap<String, String> stringOptions;

	private final HashMap<String, File> fileOptions;

	private final HashMap<String, Float> floatOptions;

	private final HashMap<String, Short> shortOptions;

	private final HashMap<String, String[]> stringArrayOptions;

	private final HashMap<String, File[]> fileArrayOptions;

	private final HashMap<String, Float[]> floatArrayOptions;

	private final HashMap<String, Short[]> shortArrayOptions;

	private final HashMap<String, String> errorMessages;

	public ParsedOptions() {
		// init calar option containers
		this.stringOptions = new HashMap<String, String>();
		this.fileOptions = new HashMap<String, File>();
		this.floatOptions = new HashMap<String, Float>();
		this.shortOptions = new HashMap<String, Short>();
		// init array option containers
		this.stringArrayOptions = new HashMap<String, String[]>();
		this.fileArrayOptions = new HashMap<String, File[]>();
		this.floatArrayOptions = new HashMap<String, Float[]>();
		this.shortArrayOptions = new HashMap<String, Short[]>();
		this.errorMessages = new HashMap<String, String>();
	}

	/**
	 * @return
	 */
	public boolean success() {
		return this.errorMessages.isEmpty();
	}

	/**
	 * @param string
	 * @param message
	 */
	public void setErrorMessage(String optionKey, String message) {
		this.errorMessages.put(optionKey, message);
	}

	/**
	 * @return
	 */
	public Collection<String> getErrorMessages() {
		return this.errorMessages.values();
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public String getString(String optionKey) {
		return this.stringOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setString(String optionKey, String option) {
		this.stringOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public File getFile(String optionKey) {
		return this.fileOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setFile(String optionKey, File option) {
		this.fileOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public Float getFloat(String optionKey) {
		return this.floatOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setFloat(String optionKey, Float option) {
		this.floatOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public Short getShort(String optionKey) {
		return this.shortOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setShort(String optionKey, Short option) {
		this.shortOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public String[] getStringArray(String optionKey) {
		return this.stringArrayOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setStringArray(String optionKey, String[] option) {
		this.stringArrayOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public File[] getFileArray(String optionKey) {
		return this.fileArrayOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setFileArray(String optionKey, File[] option) {
		this.fileArrayOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public Float[] getFloatList(String optionKey) {
		return this.floatArrayOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setFloatArray(String optionKey, Float[] option) {
		this.floatArrayOptions.put(optionKey, option);
	}

	/**
	 * @param optionKey
	 * @return
	 */
	public Short[] getShortList(String optionKey) {
		return this.shortArrayOptions.get(optionKey);
	}

	/**
	 * @param optionKey
	 * @param option
	 */
	public void setShortArray(String optionKey, Short[] option) {
		this.shortArrayOptions.put(optionKey, option);
	}
}
