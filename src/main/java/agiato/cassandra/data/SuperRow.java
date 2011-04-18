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
