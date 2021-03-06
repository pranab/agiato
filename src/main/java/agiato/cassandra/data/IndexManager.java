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

import agiato.cassandra.meta.ColumnFamilyDef;
import agiato.cassandra.meta.IndexDef;
import agiato.cassandra.meta.MetaDataManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 *
 * @author pranab
 */
public class IndexManager {
    private static  IndexManager indexmanager = new IndexManager();
    
    public static IndexManager instance(){
        return indexmanager;
    } 
     
    public void createIndex(String colFamily,ByteBuffer rowKey, List<ColumnValue> colVals) 
        throws Exception{
        MetaDataManager metaDatamanager = MetaDataManager.instance();
        ColumnFamilyDef colFamDef = metaDatamanager.findColFamilyByName(colFamily);
        List<IndexColPair> indexColList = new ArrayList<IndexColPair>();
             
        //find all indexes
        for (IndexDef indexDef : metaDatamanager.getIndexes()){
            String indexedColName = indexDef.getIndexedColumnName();
            ColumnValue colVal = findColumnValue(indexedColName, colVals);
            if (null != colVal){
                indexColList.add(new IndexColPair(indexDef, colVal));
            }
        }
        
        //create them
        for (IndexColPair indexColPair : indexColList){
            IndexDef indexDef = indexColPair.indexDef;
            String indexColFam = indexDef.getColFamilyName();
            DataAccess dataAccess = new DataAccess(indexColFam);
            ColumnValue indexColVal = indexColPair.colVal;
            
            if (indexDef.isCatIndexName()){
                List<SuperColumnValue> superColValues = new ArrayList<SuperColumnValue>();
                SuperColumnValue superColVal = new SuperColumnValue();
                superColVal.setName(indexColVal.getValue());

                List<ColumnValue> colValues = new ArrayList<ColumnValue>();
                ColumnValue colVal = new ColumnValue();
                
                if (indexDef.isStructureRowKey() || indexDef.isStructureCachedCol()) {
                    //row key in col name
                    colVal.setName(rowKey);
                    if (indexDef.isStructureCachedCol()){
                        String cachedColName = indexDef.getCachedColumnName();
                        ColumnValue cachedColVal = findColumnValue(cachedColName, colVals);
                        colVal.setValue(cachedColVal.getValue());
                    }
                    colValues.add(colVal);
                } else {
                    //col value in col name
                }
                colValues.add(colVal);
                superColVal.setValues(colValues);
                superColValues.add(superColVal);
                
                dataAccess.insertSuperColumns(indexDef.getName(), superColValues, ConsistencyLevel.ONE); 
            }

        }
        
    }
    
    private ColumnValue findColumnValue(String colName, List<ColumnValue> colVals) throws Exception{
        ColumnValue colValue = null;
        for (ColumnValue thisColVal : colVals){
            String thisColName = Util.getStringFromByteBuffer(thisColVal.getName());
            if (colName.equals(thisColName)){
                colValue = thisColVal;
                break;
            }
        }
        return colValue;
    }
    
    private static class IndexColPair {
        public IndexDef indexDef;
        public ColumnValue colVal;
        

        public IndexColPair(IndexDef indexDef, ColumnValue colVal) {
            this.indexDef = indexDef;
            this.colVal = colVal;
        }
        
        
    }

}
