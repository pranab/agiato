/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.data;

import agiato.cassandra.connect.Connector;
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
 *
 * @author pranab
 */
public class DataAccess {
    
    private DataManager dataManager  = DataManager.instance();
    
    public  ColumnValue  retrieveSubColumn(String colFamilly, String rowKey,  ByteBuffer superColName, 
        ByteBuffer colName, ConsistencyLevel consLevel)
        throws Exception {
            
        //Cassandra.Client   client = Connector.instance().openConnection();
        GenericObjectPool connectionPool = dataManager.getConnectionPool();    
        Connector connector = (Connector)connectionPool.borrowObject();
        Cassandra.Client client = connector.openConnection();
        ColumnValue colVal = null;
        
        try {
            ColumnPath colPath = new ColumnPath(colFamilly);
            colPath.setSuper_column(superColName);
            colPath.setColumn(colName);

            ColumnOrSuperColumn result =  client.get(Util.getByteBufferFromString(rowKey), colPath,  consLevel);
            colVal = new ColumnValue();
            Column col = result.getColumn();
            if (null != col){
                colVal.setName(col.BufferForName());
                colVal.setValue(col.BufferForValue());
            }

            col.BufferForName();
        } finally {
            dataManager.returnConnection(connectionPool, connector);
        }
        return colVal;
    }

    public  SuperColumnValue  retrieveSuperColumn(String colFamilly, String rowKey,  ByteBuffer superCol,
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
                    colVal.setName(col.BufferForName());
                    colVal.setValue(col.BufferForValue());
                    colValues.add(colVal);
                }
            }
            superColVal.setValues(colValues);
        } finally {
            dataManager.returnConnection(connector);
        }

        return superColVal;
    }
    
    public  List<SuperColumnValue>  retrieveSuperColumns(String colFamilly, long rowKey,  List<ByteBuffer> superCols, 
        boolean isRange, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSuperColumns(colFamilly, Util.getByteBufferFromLong(rowKey), superCols, isRange, consLevel);    
    }    

    public  List<SuperColumnValue>  retrieveSuperColumns(String colFamilly, String rowKey,  List<ByteBuffer> superCols, 
        boolean isRange, ConsistencyLevel consLevel)
        throws Exception {
        return retrieveSuperColumns(colFamilly, Util.getByteBufferFromString(rowKey), superCols, isRange, consLevel);    
    }    

    public  List<SuperColumnValue>  retrieveSuperColumns(String colFamilly, ByteBuffer rowKey,  List<ByteBuffer> superCols, 
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
                    superColVal.setName(superCol.BufferForName());
                    List<ColumnValue> colValues = new ArrayList<ColumnValue>();
                    for (Column col : superCol.getColumns()) {
                        ColumnValue colVal = new ColumnValue();
                        colVal.setName(col.BufferForName());
                        colVal.setValue(col.BufferForValue());
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
    
    public  void  updateSuperColumns(String colFamilly, String rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel)
        throws Exception{
        updateSuperColumns(colFamilly, Util.getByteBufferFromString(rowKey), superColVals, consLevel);
    }
    
    public  void  updateSuperColumns(String colFamilly, long rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel)
        throws Exception{
        updateSuperColumns(colFamilly, Util.getByteBufferFromLong(rowKey), superColVals, consLevel);
    }

    public  void  updateSuperColumns(String colFamilly, ByteBuffer rowKey, List<SuperColumnValue> superColVals,
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
                    columns.add(new Column(colVal.getName(), colVal.getValue(), timestamp));
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

    public  void insertSuperColumns(String colFamilly, ByteBuffer rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel) 
        throws Exception {
        updateSuperColumns(colFamilly, rowKey, superColVals, consLevel);
    }
    
  
    public  void insertSuperColumns(String colFamilly, String rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel) 
        throws Exception {
        updateSuperColumns(colFamilly, rowKey, superColVals, consLevel);
    }

    public  void insertSuperColumns(String colFamilly, long rowKey, List<SuperColumnValue> superColVals,
        ConsistencyLevel consLevel) 
        throws Exception {
        updateSuperColumns(colFamilly, rowKey, superColVals, consLevel);
    }

    public  List<ColumnValue>  retrieveColumns(String colFamilly, String rowKey, ConsistencyLevel consLevel)
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

    public  List<SimpleRow>  queryColumns(String colFamilly, ByteBuffer column, IndexOperator operator, 
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
                ByteBuffer key = ks.BufferForKey();
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
    

    public  void insertColumns(String colFamilly, String rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
        throws Exception {
        updateColumns(colFamilly, rowKey, colVals, consLevel);    
    }

    public  void insertColumns(String colFamilly, long rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
        throws Exception {
        updateColumns(colFamilly, rowKey, colVals, consLevel);    
    }

    public   void  updateColumns(String colFamilly, String rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
         throws Exception{
        updateColumns(colFamilly, Util.getByteBufferFromString(rowKey), colVals, consLevel);
    }
    
    public   void  updateColumns(String colFamilly, long rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
        throws Exception{
        updateColumns(colFamilly, Util.getByteBufferFromLong(rowKey), colVals, consLevel);
    }
    
    public   void  updateColumns(String colFamilly, ByteBuffer rowKey, List<ColumnValue> colVals, ConsistencyLevel consLevel)
         throws Exception{
        Connector connector = dataManager.borrowConnection();
        Cassandra.Client client = connector.openConnection();

        try{
            long timestamp = System.currentTimeMillis();

            Map<ByteBuffer, Map<String, List<Mutation>>> job = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            List<Mutation> mutations = new ArrayList<Mutation>();

            List<Column> columns = new ArrayList<Column>();
            for (ColumnValue colVal : colVals){
                Column col = new Column(colVal.getName(), colVal.getValue(), timestamp);
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

    private List<ColumnValue> getColumns(List<ColumnOrSuperColumn> colSuperCols){
        List<ColumnValue> colValues = new ArrayList<ColumnValue>();
        for (ColumnOrSuperColumn colSup : colSuperCols){
            Column col = colSup.getColumn();
            if (null != col){
                ColumnValue colVal = new ColumnValue();
                colVal.setName(col.BufferForName());
                colVal.setValue(col.BufferForValue());
                colValues.add(colVal);
            }
        }
        return colValues;
    }
    
    private SlicePredicate slicePredicateWithAllCol(){
        SlicePredicate slicePredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(ByteBuffer.allocate(0));
        sliceRange.setFinish(ByteBuffer.allocate(0));
        sliceRange.reversed = false;
        slicePredicate.setSlice_range(sliceRange);
        
        return slicePredicate;
    }

}
