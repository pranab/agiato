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

package agiato.cassandra.data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;

public class BatchLoader {
	private int batchSize;
	private int rowCount;
	private ConsistencyLevel consLevel;
	private List<SuperRow> superRows = new ArrayList<SuperRow>();
	private DataAccess dataAccess;
	
	public BatchLoader(String table, int batchSize, ConsistencyLevel consLevel) {
		this.batchSize = batchSize;
		this.consLevel = consLevel;
		dataAccess = new DataAccess(table);
	}
	
	public void addRow(ByteBuffer rowKey, List<SuperColumnValue> superColVals) throws Exception {
		SuperRow superRow = new SuperRow(rowKey, superColVals);
		superRows.add(superRow);
		if (++rowCount == batchSize) {
			dataAccess.batchUpdateSuperColumns(superRows, consLevel);
			rowCount = 0;
			superRows.clear();
		}
	}
	
	public void  close() throws Exception {
		dataAccess.batchUpdateSuperColumns(superRows, consLevel);
		rowCount = 0;
		superRows.clear();
	}
	
}
