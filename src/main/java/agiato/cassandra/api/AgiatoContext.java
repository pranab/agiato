/*
 * Agiato: A simple no frill Cassandra API
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package agiato.cassandra.api;

import agiato.cassandra.data.DataAccess;
import agiato.cassandra.data.DataManager;

/**
 * Entry point to API, proving factory methods for reader and writer
 * @author pranab
 *
 */
public class AgiatoContext {
	
	/**
	 * @param configFile
	 * @throws Exception
	 */
	public static void initialize(String configFile) throws Exception {
		DataManager.initialize(configFile, false);
	}
	
	/**
	 * create column family writer
	 * @param colFamilly
	 * @return
	 */
	public static ColumnFamilyWriter createWriter(String colFamilly) {
		DataAccess datAcc =  new DataAccess(colFamilly);
		return (ColumnFamilyWriter)datAcc;
	}
	
	/**
	 * create column family reader
	 * @param colFamilly
	 * @return
	 */
	public static ColumnFamilyReader  createReader(String colFamilly) {
		DataAccess datAcc =  new DataAccess(colFamilly);
		return (ColumnFamilyReader)datAcc;
	}
	
}
