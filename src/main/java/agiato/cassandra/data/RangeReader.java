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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Performs range query. Dynamically changes column range based number of columns returned from
 * the last query. Tries to return batchSize number of columns 
 * @author pranab
 *
 */
public class RangeReader {
	private  ByteBuffer rowKey;
	private int batchSize;
	private int maxFetchSize;
	private ByteBuffer startCol;
	private Long startColLong;
	private ByteBuffer endCol;
	private Long endColLong;
	private int curRangeSize;
	private ConsistencyLevel consLevel;
	private DataAccess dataAccess;
	private ByteBuffer superCol;
	public enum ColumnType {
		   COL_LONG, COL_STRING
	}
	private ColumnType colType;
	private List<ByteBuffer> colRange = new ArrayList<ByteBuffer>(2);
	private int lastFetchCount = -1;
	private int batchSizeMin;
	private int batchSizeMax;
	private List<ColumnValue> colValues;
	private boolean atRowEnd;
	private static ByteBuffer endMarker;
	
	/**
	 * @param colFam col family name
	 * @param rowKey row key which is Long, String or ByteBuffer (if row key is a composite) 
	 * @param batchSize desired number of columns
	 * @param batchSizeTolerance batch size tolerance as a percentage of batch size (e.g. 10)
	 * @param maxFetchSize max number of columns to fetch 
	 * @param startCol start col value
	 * @param initialRangeSize initial number of columns to fetch
	 * @param consLevel consistency level
	 * @param superCol super column name, null if there is no super column
	 * @param colType column name data type
	 * @throws IOException
	 */
	public RangeReader(String colFam, Object rowKey, int batchSize, int batchSizeTolerance, int  maxFetchSize, Object startCol, 
		int initialRangeSize,  ConsistencyLevel consLevel, Object  superCol, ColumnType colType) throws IOException {
		this.dataAccess = new DataAccess(colFam);
		this.rowKey = getByteBuffer(rowKey, false);
		this.batchSize = batchSize;
		this.maxFetchSize = maxFetchSize;
		this.startCol = getByteBuffer(startCol, true);
		this.consLevel  = consLevel;
		if (null != superCol) {
			this.superCol= getByteBuffer(superCol, false);
		}
		this.colType = colType;
		if (this.colType == ColumnType.COL_LONG) {
			startColLong = (Long)startCol;
		}
		curRangeSize = initialRangeSize;
		batchSizeMin = ((100 - batchSizeTolerance) * batchSize) / 100;
		batchSizeMax = ((100 +batchSizeTolerance) * batchSize) / 100;
		if (null == endMarker) {
			endMarker = Util.getByteBufferFromString("");
		}
	}
	
	/**
	 * reads columns
	 * @return
	 * @throws Exception
	 */
	public List<ColumnValue> getColumnValues() throws Exception {
		if (!atRowEnd) {
		    //set column range
			setEndCol();
			colRange.clear();
			colRange.add(startCol);
			colRange.add(endCol);
			
			//range query
			colValues =  dataAccess.retrieveSubColumns( rowKey,   superCol, colRange, true, maxFetchSize, consLevel);		
			atRowEnd = endCol == endMarker &&  colValues.size() <  maxFetchSize;
			lastFetchCount = colValues.size();
			
			//reset start column
			setStartCol();
		} else {
			colValues = null;
		}
		return colValues;
	}
	
	/**
	 * @param obj object to be serialized
	 * @param acceptTyped true if only primitive typed object accepted
	 * @return
	 * @throws IOException
	 */
	private ByteBuffer getByteBuffer(Object obj, boolean acceptTyped) throws IOException {
		ByteBuffer bytBuf = null;
		if (obj instanceof Long){
			bytBuf= Util.getByteBufferFromLong((Long)obj);
		} else if (obj instanceof String){
			bytBuf= Util.getByteBufferFromString((String)obj);
		} else if (!acceptTyped && obj instanceof ByteBuffer){
			bytBuf=(ByteBuffer) obj;
		} else {
			throw new IOException("Unspported column or super column type");
		}
		return bytBuf;
	}
	
	/**
	 * sets end column
	 * @throws IOException
	 */
	private void setEndCol() throws IOException {
		if (colType == ColumnType.COL_LONG) {
			if (lastFetchCount >= 0) {
				if (lastFetchCount > batchSizeMax || lastFetchCount < batchSizeMin) {
					//increase or reduce range
					curRangeSize = lastFetchCount == 0 ? curRangeSize * 2  : (batchSize  * curRangeSize) / lastFetchCount;
				} 
			}
			if (Long.MAX_VALUE - curRangeSize > startColLong){
				endColLong = startColLong + curRangeSize;
				endCol = Util.getByteBufferFromLong(endColLong);
			} else {
				endCol =endMarker;
			}
		}
	}
	
	/**
	 * sets start column
	 * @throws IOException
	 */
	private void setStartCol() throws IOException {
		if (lastFetchCount > 0) {
			ColumnValue colVal = colValues.get(colValues.size()-1);
			if (colType == ColumnType.COL_LONG) {
				startColLong = Util.getLongFromByteBuffer(colVal.getName()) + 1;
				startCol = Util.getByteBufferFromLong(startColLong);
			}			
		} else {
			if (colType == ColumnType.COL_LONG) {
				startColLong = endColLong + 1;
				startCol = Util.getByteBufferFromLong(startColLong);
			}			
		}
	}

	/**
	 * @param rowKey
	 */
	public void setRowKey(ByteBuffer rowKey) {
		this.rowKey = rowKey;
	}
	
	/**
	 * @param startCol
	 * @throws IOException
	 */
	public void setStartCol(Object startCol) throws IOException {
		this.startCol = getByteBuffer(startCol, true);
	}
	
	/**
	 * @param initialRangeSize
	 */
	public void setInitialRangeSize(int initialRangeSize) {
		curRangeSize = initialRangeSize;
	}
	
	/**
	 * @param superCol
	 * @throws IOException
	 */
	public void setSuperCol(Object  superCol) throws IOException{
		this.superCol= getByteBuffer(superCol, false);
	}

	/**
	 * @return
	 */
	public boolean isAtRowEnd() {
		return atRowEnd;
	}
}
