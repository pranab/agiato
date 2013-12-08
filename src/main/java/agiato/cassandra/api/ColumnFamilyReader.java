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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;

import agiato.cassandra.data.ColumnValue;
import agiato.cassandra.data.PrimaryKey;

/**
 * interface for reading column family
 * @author pranab
 *
 */
public interface  ColumnFamilyReader {

	/**
     * Retirieves a column from stanadard CF 
     * @param rowKey
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveColumn(String rowKey, ByteBuffer colName, 
        ConsistencyLevel consLevel) throws Exception;
    
	/**
     * Retirieves a column from stanadard CF 
     * @param rowKey
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveColumn(long rowKey, ByteBuffer colName, 
        ConsistencyLevel consLevel) throws Exception;
 
	/**
     * Retirieves a column from stanadard CF 
     * @param rowKey
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveColumn(ByteBuffer rowKey, ByteBuffer colName, 
        ConsistencyLevel consLevel) throws Exception;

    /**
     * @param rowKey
     * @param superCol
     * @param cols
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumnSlice(long rowKey,   List<ByteBuffer> cols,  int limit, 
    		ConsistencyLevel consLevel)
            throws Exception;

    /**
     * @param rowKey
     * @param superCol
     * @param cols
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumnSlice(String rowKey,   List<ByteBuffer> cols,  int limit, 
    		ConsistencyLevel consLevel)
            throws Exception;
    /**
     * @param rowKey
     * @param superCol
     * @param cols
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumnSlice(ByteBuffer rowKey,   List<ByteBuffer> cols,  int limit, 
    		ConsistencyLevel consLevel)
            throws Exception;

    /**
     * @param rowKey
     * @param superCol
     * @param cols
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumnRange(long rowKey,   List<ByteBuffer> cols,  int limit, 
    		ConsistencyLevel consLevel)
            throws Exception;
    
    /**
     * @param rowKey
     * @param superCol
     * @param cols
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumnRange(String rowKey,   List<ByteBuffer> cols,  int limit, 
    		ConsistencyLevel consLevel)
            throws Exception;

    /**
     * @param rowKey
     * @param superCol
     * @param cols
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumnRange(ByteBuffer  rowKey,   List<ByteBuffer> cols,  int limit, 
    		ConsistencyLevel consLevel)
            throws Exception;

    /**
     * @param obj
     * @param primKey
     * @param consLevel
     * @throws Exception
     */
    public   List<Object>  retrieveObject(Object obj, PrimaryKey primKey, ConsistencyLevel consLevel) throws Exception;
}
