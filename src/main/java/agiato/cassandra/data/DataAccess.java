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

import agiato.cassandra.connect.Connector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * Data  read write accessor
 * @author pranab
 */
public class DataAccess {
    private String colFamilly;
    private DataManager dataManager  = DataManager.instance();

    /**
     * Constructor
     * @param colFamilly
     */
    public DataAccess(String colFamilly) {
        this.colFamilly = colFamilly;
    }
    
    /**
     * Retirieves a column from stanadard CF 
     * @param rowKey
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveColumn(String rowKey, ByteBuffer colName, 
        ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumn(rowKey, null,  colName,  consLevel);
    }

    /**
     * Retirieves a column from stanadard CF 
     * @param rowKey
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveColumn(long rowKey, ByteBuffer colName, 
        ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumn(rowKey, null,  colName,  consLevel);
    }

    /**
     * Retirieves a column from stanadard CF 
     * @param rowKey
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveColumn(ByteBuffer rowKey,   
        ByteBuffer colName, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumn(rowKey, null,  colName,  consLevel);
    }

    /**
     * Retirieves a column from super CF 
     * @param rowKey
     * @param superColName
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveSubColumn(String rowKey, ByteBuffer superColName, 
        ByteBuffer colName, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumn(Util.getByteBufferFromString(rowKey),  superColName, 
            colName, consLevel);
    }    
    
    /**
     * Retirieves a column from super CF 
     * @param rowKey
     * @param superColName
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveSubColumn(long rowKey, ByteBuffer superColName, 
        ByteBuffer colName, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumn(Util.getByteBufferFromLong(rowKey),  superColName, 
            colName, consLevel);
    }    
    
    /**
     * Retirieves a column from super CF 
     * @param rowKey
     * @param superColName
     * @param colName
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  ColumnValue  retrieveSubColumn(ByteBuffer rowKey,  ByteBuffer superColName, 
        ByteBuffer colName, ConsistencyLevel consLevel)
        throws Exception {
            
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();
        ColumnValue colVal = null;
        
        try {
            ColumnPath colPath = new ColumnPath(colFamilly);
            if (null != superColName){
                colPath.setSuper_column(superColName);
            }
            colPath.setColumn(colName);

            ColumnOrSuperColumn result =  client.get(rowKey, colPath,  consLevel);
            colVal = new ColumnValue();
            Column col = result.getColumn();
            if (null != col){
                colVal.setName(col.bufferForName());
                colVal.setValue(col.bufferForValue());
            }

        } finally {
            dataManager.returnConnection(connector);
        }
        return colVal;
    }
    
    /**
     * Retirieves multiple columns from standard CF 
     * @param rowKey
     * @param superCol
     * @param cols
     * @param isRange
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumns(long rowKey,  ByteBuffer superCol, 
        List<ByteBuffer> cols, boolean isRange, int limit, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumns(rowKey,  null, cols,  isRange, limit, consLevel);
    }

    /**
     * Retirieves multiple columns from standard CF 
     * @param rowKey
     * @param superCol
     * @param cols
     * @param isRange
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumns(String rowKey,  ByteBuffer superCol, 
        List<ByteBuffer> cols, boolean isRange, int limit, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumns(rowKey,  null, cols,  isRange, limit, consLevel);
    }

    /**
     * Retirieves multiple columns from standard CF 
     * @param rowKey
     * @param superCol
     * @param cols
     * @param isRange
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumns(ByteBuffer rowKey,  ByteBuffer superCol, 
        List<ByteBuffer> cols, boolean isRange, int limit, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumns(rowKey,  null, cols,  isRange, limit, consLevel);
    }
    
    /**
     * Retirieves multiple columns from super CF 
     * @param rowKey
     * @param superCol
     * @param cols
     * @param isRange
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveSubColumns(String rowKey,  ByteBuffer superCol, 
        List<ByteBuffer> cols, boolean isRange, int limit, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumns(Util.getByteBufferFromString(rowKey),  superCol,  cols,  isRange, limit, consLevel);
    }
    
    /**
     * Retirieves multiple columns from super CF 
     * @param rowKey
     * @param superCol
     * @param cols
     * @param isRange
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveSubColumns(long rowKey,  ByteBuffer superCol, 
        List<ByteBuffer> cols, boolean isRange, int limit, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSubColumns(Util.getByteBufferFromLong(rowKey),  superCol,  cols,  isRange,  limit, consLevel);
    }

    /**
     * Retirieves multiple columns from super CF 
     * @param rowKey
     * @param superCol
     * @param cols
     * @param isRange
     * @param limit
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveSubColumns(ByteBuffer rowKey,  ByteBuffer superCol, 
        List<ByteBuffer> cols, boolean isRange, int limit, ConsistencyLevel consLevel)
        throws Exception {
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();
        List<ColumnValue> colVals = null;
        
        try{
            SlicePredicate slicePredicate = new SlicePredicate();
            if (isRange) {
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(cols.get(0));
                sliceRange.setFinish(cols.get(1));
                slicePredicate.setSlice_range(sliceRange);
                if (limit > 0){
                	sliceRange.setCount(limit);
                }
            } else {
                slicePredicate.setColumn_names(cols);
            }

            ColumnParent colPar = new ColumnParent(colFamilly);
            if (null != superCol){
                colPar.setSuper_column(superCol);
            }
            List<ColumnOrSuperColumn> result =  client.get_slice(rowKey, colPar, slicePredicate, consLevel);

            colVals = new ArrayList<ColumnValue>();
            for (ColumnOrSuperColumn colSup : result){
                Column col = colSup.getColumn();
                if (null != col){
                    ColumnValue colVal = new ColumnValue();
                    colVal.setName(col.bufferForName());
                    colVal.setValue(col.bufferForValue());
                    colVals.add(colVal);
                }
            }
        } finally {
            dataManager.returnConnection(connector);
        }

        return colVals;
    }
    
    /**
     * Retirieves super column from super CF 
     * @param rowKey
     * @param superCol
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  SuperColumnValue  retrieveSuperColumn(String rowKey,  ByteBuffer superCol,
        ConsistencyLevel consLevel)
        throws Exception {
            
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();
        SuperColumnValue superColVal = null;
        
        try {
            SlicePredicate slicePredicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(ByteBuffer.allocate(0));
            sliceRange.setFinish(ByteBuffer.allocate(0));
            slicePredicate.setSlice_range(sliceRange);

            ColumnParent colPar = new ColumnParent(colFamilly);
            colPar.setSuper_column(superCol);
            List<ColumnOrSuperColumn> result =  client.get_slice(Util.getByteBufferFromString(rowKey), colPar, slicePredicate, consLevel);

            superColVal = new SuperColumnValue();
            superColVal.setName(superCol);
            List<ColumnValue> colValues = new ArrayList<ColumnValue>();

            for (ColumnOrSuperColumn colSup : result){
                Column col = colSup.getColumn();
                if (null != col){
                    ColumnValue colVal = new ColumnValue();
                    colVal.setName(col.bufferForName());
                    colVal.setValue(col.bufferForValue());
                    colValues.add(colVal);
                }
            }
            superColVal.setValues(colValues);
        } finally {
            dataManager.returnConnection(connector);
        }

        return superColVal;
    }
    
    /**
     * Retirieves multiple super columns from super CF 
     * @param rowKey
     * @param superCols
     * @param isRange
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<SuperColumnValue>  retrieveSuperColumns(long rowKey,  List<ByteBuffer> superCols, 
        boolean isRange, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSuperColumns(Util.getByteBufferFromLong(rowKey), superCols, isRange, consLevel);    
    }    

    /**
     * Retirieves multiple super columns from super CF 
     * @param rowKey
     * @param superCols
     * @param isRange
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<SuperColumnValue>  retrieveSuperColumns(String rowKey,  List<ByteBuffer> superCols, 
        boolean isRange, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSuperColumns(Util.getByteBufferFromString(rowKey), superCols, isRange, consLevel);    
    }    
    
    /**
     * Retirieves multiple super columns from super CF 
     * @param rowKey
     * @param superCols
     * @param isRange
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<SuperColumnValue>  retrieveSuperColumns(ByteBuffer rowKey,  List<ByteBuffer> superCols, 
        boolean isRange, ConsistencyLevel consLevel)
        throws Exception {
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();
        List<SuperColumnValue> superColVals = null;
        
        try{
            SlicePredicate slicePredicate = new SlicePredicate();
            if (isRange) {
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(superCols.get(0));
                sliceRange.setFinish(superCols.get(1));
                slicePredicate.setSlice_range(sliceRange);
            } else {
                slicePredicate.setColumn_names(superCols);
            }

            ColumnParent colPar = new ColumnParent(colFamilly);
            List<ColumnOrSuperColumn> result =  client.get_slice(rowKey, colPar, slicePredicate, consLevel);

            superColVals = new ArrayList<SuperColumnValue>();
            for (ColumnOrSuperColumn colSup : result){
                SuperColumn superCol = colSup.getSuper_column();
                if (null != superCol){
                    SuperColumnValue superColVal = new SuperColumnValue();
                    superColVal.setName(superCol.bufferForName());
                    List<ColumnValue> colValues = new ArrayList<ColumnValue>();
                    for (Column col : superCol.getColumns()) {
                        ColumnValue colVal = new ColumnValue();
                        colVal.setName(col.bufferForName());
                        colVal.setValue(col.bufferForValue());
                        colValues.add(colVal);
                    }
                    superColVal.setValues(colValues);
                    superColVals.add(superColVal);
                }
            }
        } finally {
            dataManager.returnConnection(connector);
        }

        return superColVals;
    }
    
    /**
     * Updates multiple super columns in super CF 
     * @param rowKey
     * @param superColVals
     * @param consLevel
     * @throws Exception
     */
    public  void  updateSuperColumns(String rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel)
        throws Exception{
        updateSuperColumns(Util.getByteBufferFromString(rowKey), superColVals, consLevel);
    }
    
    /**
     * Updates multiple super columns in super CF 
     * @param rowKey
     * @param superColVals
     * @param consLevel
     * @throws Exception
     */
    public  void  updateSuperColumns(long rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel)
        throws Exception{
        updateSuperColumns(Util.getByteBufferFromLong(rowKey), superColVals, consLevel);
    }
    
    /**
     * Updates multiple super columns in super CF 
     * @param rowKey
     * @param superColVals
     * @param consLevel
     * @throws Exception
     */
    public  void  updateSuperColumns(ByteBuffer rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel)
        throws Exception{
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();

        try{
            System.out.println("colFamilly: " + colFamilly + " rowKey: " + rowKey);
            long timestamp = System.currentTimeMillis();
            Map<ByteBuffer, Map<String, List<Mutation>>> job = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            List<Mutation> mutations = new ArrayList<Mutation>();

            for (SuperColumnValue superColVal : superColVals ){
                ByteBuffer superCol =  superColVal.getName();
                List<ColumnValue>  cols = superColVal.getValues();

                List<Column> columns = new ArrayList<Column>();
                for (ColumnValue colVal : cols){
                    Column col = new Column(colVal.getName());
                    col.setValue(colVal.getValue());
                    col.setTimestamp(timestamp);  
                    columns.add(col);
                }

                
                SuperColumn superColumn = new SuperColumn(superCol, columns);
                ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
                columnOrSuperColumn.setSuper_column(superColumn);

                Mutation mutation = new Mutation();
                mutation.setColumn_or_supercolumn(columnOrSuperColumn);
                mutations.add(mutation);
            }


            Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
            mutationsForColumnFamily.put(colFamilly, mutations);

            job.put(rowKey, mutationsForColumnFamily);
            client.batch_mutate(job, consLevel);
        } finally {
            dataManager.returnConnection(connector);
        }
    }
    
    /**
     * Inserts multiple super columns in super CF 
     * @param rowKey
     * @param superColVals
     * @param consLevel
     * @throws Exception
     */
    public  void insertSuperColumns(ByteBuffer rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel) 
        throws Exception {
        updateSuperColumns(rowKey, superColVals, consLevel);
    }
    
    /**
     * Inserts multiple super columns in super CF 
     * @param rowKey
     * @param superColVals
     * @param consLevel
     * @throws Exception
     */
    public  void insertSuperColumns(String rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel) 
        throws Exception {
        updateSuperColumns(rowKey, superColVals, consLevel);
    }
    
    /**
     * Inserts multiple super columns in super CF 
     * @param rowKey
     * @param superColVals
     * @param consLevel
     * @throws Exception
     */
    public  void insertSuperColumns(long rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel) 
        throws Exception {
        updateSuperColumns(rowKey, superColVals, consLevel);
    }
    
    /**
     * Batch updates multiple super column rows  
     * @param superRows
     * @param consLevel
     * @throws Exception
     */
    public  void  batchUpdateSuperColumns(List<SuperRow> superRows, ConsistencyLevel consLevel)
        throws Exception{
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();

        try{
            long timestamp = System.currentTimeMillis();
            Map<ByteBuffer, Map<String, List<Mutation>>> job = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
	    	for (SuperRow superRow : superRows) {
	    		 Map<String, List<Mutation>> mutations = getMutations(superRow.getSuperColValues(),  timestamp);
	             job.put(superRow.getKey(), mutations);
	    	}	
            client.batch_mutate(job, consLevel);
	    } finally {
	             dataManager.returnConnection(connector);
	     }    	
    }
    
    /**
     * Batch updates multiple super column rows  
     * @param superRows
     * @param consLevel
     * @throws Exception
     */
    public  void  batchInsertSuperColumns(List<SuperRow> superRows,
        ConsistencyLevel consLevel)
        throws Exception{
    	batchUpdateSuperColumns(superRows, consLevel);
    }
    
    /**
     * @param superColVals
     * @param timestamp
     * @return
     */
    private Map<String, List<Mutation>> getMutations(List<SuperColumnValue> superColVals, long timestamp) {
        List<Mutation> mutations = new ArrayList<Mutation>();

        for (SuperColumnValue superColVal : superColVals ){
            ByteBuffer superCol =  superColVal.getName();
            List<ColumnValue>  cols = superColVal.getValues();

            List<Column> columns = new ArrayList<Column>();
            for (ColumnValue colVal : cols){
                Column col = new Column(colVal.getName());
                col.setValue(colVal.getValue());
                col.setTimestamp(timestamp);  
                columns.add(col);
            }
            
            SuperColumn superColumn = new SuperColumn(superCol, columns);
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setSuper_column(superColumn);

            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(columnOrSuperColumn);
            mutations.add(mutation);
        }

        Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
        mutationsForColumnFamily.put(colFamilly, mutations);
    	return mutationsForColumnFamily;
    }
    
    /**
     * Retirieves all columns from standard CF 
     * @param rowKey
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<ColumnValue>  retrieveColumns(String rowKey, ConsistencyLevel consLevel)
        throws Exception {
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();
        List<ColumnValue> colValues = null;
        
        try{
        SlicePredicate slicePredicate = slicePredicateWithAllCol();

        ColumnParent colPar = new ColumnParent(colFamilly);
        List<ColumnOrSuperColumn> colSuperCols =  client.get_slice(Util.getByteBufferFromString(rowKey), colPar, slicePredicate, consLevel);
        
        colValues = getColumns(colSuperCols);
        } finally {
            dataManager.returnConnection(connector);
        }
        return colValues;
     }

    /**
     * Queries all rows from standard CF using native index
     * @param column
     * @param operator
     * @param colValue
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<SimpleRow>  queryColumns(ByteBuffer column, IndexOperator operator, 
            ByteBuffer colValue, ConsistencyLevel consLevel)
        throws Exception {
        List<SimpleRow> rows = new ArrayList<SimpleRow>();
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();
        
        try {
            ColumnParent columnParent = new ColumnParent();
            columnParent.column_family = colFamilly;

            IndexClause indexClause = new IndexClause();
            indexClause.start_key = ByteBuffer.allocate(0);
            IndexExpression indexExpression = new IndexExpression();
            indexExpression.column_name = column;
            indexExpression.value = colValue;
            indexExpression.op = operator;
            indexClause.addToExpressions(indexExpression);

            SlicePredicate slicePredicate = slicePredicateWithAllCol();
            List<KeySlice> keys = client.get_indexed_slices(columnParent, indexClause, slicePredicate, consLevel);

            for (KeySlice ks : keys){
                SimpleRow row = new SimpleRow();
                ByteBuffer key = ks.bufferForKey();
                List<ColumnOrSuperColumn> colSuperCols = ks.getColumns();

                List<ColumnValue> colValues = getColumns(colSuperCols);
                row.setKey(key);
                row.setColValues(colValues);

            }
        } finally {
            dataManager.returnConnection(connector);
        }
            
        return rows;
    }
    
    /**
     * Queries all rows from standard CF using agiato index
     * @param query
     * @param args
     * @param consLevel
     * @return
     * @throws Exception
     */
    public  List<SimpleRow> queryColumns(String query, List<Object> args, ConsistencyLevel consLevel)
        throws Exception {
        List<SimpleRow> rows = null;
         //TODO
        
        return rows;
     }    

    
    /**
     * Inserts column values in standard CF 
     * @param rowKey
     * @param colVals
     * @param consLevel
     * @throws Exception
     */
    public  void insertColumns(String rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
        throws Exception {
        updateColumns(rowKey, colVals, consLevel);   
        IndexManager.instance().createIndex(colFamilly, Util.getByteBufferFromString(rowKey), colVals);
    }

    /*
     * Inserts columnn values in standard CF 
     */
    public  void insertColumns(long rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
        throws Exception {
        updateColumns(rowKey, colVals, consLevel);    
        IndexManager.instance().createIndex(colFamilly, Util.getByteBufferFromLong(rowKey), colVals);
    }

    /**
     * @param obj
     * @param rowKeyCompCount
     * @param primKeyCompnentCount
     * @param consLevel
     * @throws Exception
     */
    public   void  insertObject(ObjectNode obj, int rowKeyCompCount, int primKeyCompnentCount, 
    		ConsistencyLevel consLevel) throws Exception {
    	updateObject( obj,  rowKeyCompCount,  primKeyCompnentCount, consLevel);
    }
    
    /**
     * Updates column values in standard CF 
     * @param rowKey
     * @param colVals
     * @param consLevel
     * @throws Exception
     */
    public   void  updateColumns(String rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
         throws Exception{
        updateColumns(Util.getByteBufferFromString(rowKey), colVals, consLevel);
    }
    
    /**
     * Updates columnn values in standard CF 
     * @param rowKey
     * @param colVals
     * @param consLevel
     * @throws Exception
     */
    public   void  updateColumns(long rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
        throws Exception{
        updateColumns(Util.getByteBufferFromLong(rowKey), colVals, consLevel);
    }
    
    /**
     * Updates column values in standard CF 
     * @param rowKey
     * @param colVals
     * @param consLevel
     * @throws Exception
     */
    public   void  updateColumns(ByteBuffer rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
         throws Exception{
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();

        try{
            long timestamp = System.currentTimeMillis();

            Map<ByteBuffer, Map<String, List<Mutation>>> job = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            List<Mutation> mutations = new ArrayList<Mutation>();

            List<Column> columns = new ArrayList<Column>();
            for (ColumnValue colVal : colVals){
                Column col = new Column(colVal.getName());
                col.setValue(colVal.getValue());
                col.setTimestamp(timestamp);
                
                ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
                columnOrSuperColumn.setColumn(col);
                Mutation mutation = new Mutation();
                mutation.setColumn_or_supercolumn(columnOrSuperColumn);
                mutations.add(mutation);
            }

            Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
            mutationsForColumnFamily.put(colFamilly, mutations);

            job.put(rowKey, mutationsForColumnFamily);
            client.batch_mutate(job, consLevel);
        } finally {
            dataManager.returnConnection(connector);
        }
        
    }
    
    /**
     * Updates dynamic nested  object with standard col and composite key
     * @param obj
     * @param rowKeyCompCount
     * @param primKeyCompnentCount
     * @param consLevel
     * @throws Exception
     */
    public   void  updateObject(ObjectNode obj, int rowKeyCompCount, int primKeyCompnentCount, 
    		ConsistencyLevel consLevel) throws Exception {
    	ObjectSerDes serDes = new ObjectSerDes(rowKeyCompCount,  primKeyCompnentCount);
    	serDes.deconstruct(obj);
    	serDes.serialize();
    	updateColumns(serDes.getRowKey(), serDes.getColValues(),  consLevel);
	}
    
    /**
     * returns list of standard columns
     * @param colSuperCols
     * @return
     */
    private List<ColumnValue> getColumns(List<ColumnOrSuperColumn> colSuperCols){
        List<ColumnValue> colValues = new ArrayList<ColumnValue>();
        for (ColumnOrSuperColumn colSup : colSuperCols){
            Column col = colSup.getColumn();
            if (null != col){
                ColumnValue colVal = new ColumnValue();
                colVal.setName(col.bufferForName());
                colVal.setValue(col.bufferForValue());
                colValues.add(colVal);
            }
        }
        return colValues;
    }
    
    /**
     * Returns col range to include all
     * @return
     */
    private SlicePredicate slicePredicateWithAllCol(){
        SlicePredicate slicePredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(Util.getEmptyByteBuffer());
        sliceRange.setFinish(Util.getEmptyByteBuffer());
        sliceRange.reversed = false;
        slicePredicate.setSlice_range(sliceRange);
        
        return slicePredicate;
    }

}
