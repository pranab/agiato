/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author pranab
 */
public class SuperColumnValue  extends BaseColumnValue{
    private List<ColumnValue> values;

    public void  write(String name, Map<String, String>colValues) throws Exception{
         this.name =  Util.getByteBufferFromString(name);
         values = new ArrayList<ColumnValue>();
         for (String colName : colValues.keySet()){
             ColumnValue colVal = new ColumnValue();
             colVal.write(name, colValues.get(colName));
             values.add(colVal);
         }
    }

    public Map<String, String> read() throws Exception{
        Map<String, String> columns = new HashMap<String, String>();
        for (ColumnValue colVal : values){
            String name = Util.getStringFromByteBuffer(colVal.getName());
            String value = Util.getStringFromByteBuffer(colVal.getValue());
            columns.put(name, value);
        }
        return columns;
    }


    /**
     * @return the values
     */
    public List<ColumnValue> getValues() {
        return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<ColumnValue> values) {
        this.values = values;
    }

}
