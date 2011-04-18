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
