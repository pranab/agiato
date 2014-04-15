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

import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;

import agiato.cassandra.data.ColumnValue;
import agiato.cassandra.data.PrimaryKey;
import agiato.cassandra.data.SimpleRow;

public interface ColumnFamilyWriter {

    /**
     * Low level write
     * @param row
     * @param consLevel
     * @throws Exception
     */
    public void writeRow(SimpleRow row, ConsistencyLevel consLevel) throws Exception;
    
    /**
     * Low level write of multiple row
     * @param rows
     * @param consLevel
     * @throws Exception
     */
    public void writeRows(List<SimpleRow> rows, ConsistencyLevel consLevel) throws Exception;
    
    /**
     * Write row with composite row and cluster key
     * @param row
     * @param consLevel
     * @throws Exception
     */
    public void writeRow(List<Object> rowKey, List<Object> clusterKey,  List<ColumnValue> columns,
    		ConsistencyLevel consLevel) throws Exception;
   
    /**
     * Write object modeled with composite row and cluster key
     * @param obj
     * @param primKey
     * @param consLevel
     * @throws Exception
     */
    public   void  writeObject(Object obj, PrimaryKey primKey, ConsistencyLevel consLevel) throws Exception;
    
    /**
     * Write objects modeled with composite row and cluster key
     * @param objs
     * @param primKey
     * @param consLevel
     * @throws Exception
     */
    public   void  writeObjects(List<Object> objs, PrimaryKey primKey, ConsistencyLevel consLevel) throws Exception;
	
}
