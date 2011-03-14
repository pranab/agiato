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
public class SuperRow extends BaseRow {
    private List<SuperColumnValue> superColValues = new ArrayList<SuperColumnValue>();

    /**
     * @return the superColValues
     */
    public List<SuperColumnValue> getSuperColValues() {
        return superColValues;
    }

    /**
     * @param superColValues the superColValues to set
     */
    public void setSuperColValues(List<SuperColumnValue> superColValues) {
        this.superColValues = superColValues;
    }
    

}
