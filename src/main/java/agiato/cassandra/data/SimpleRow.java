/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.data;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pranab
 */
public class SimpleRow extends BaseRow {
    private List<ColumnValue> colValues = new ArrayList<ColumnValue>();
    

    /**
     * @return the colValues
     */
    public List<ColumnValue> getColValues() {
        return colValues;
    }

    /**
     * @param colValues the colValues to set
     */
    public void setColValues(List<ColumnValue> colValues) {
        this.colValues = colValues;
    }

}
