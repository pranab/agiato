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

package agiato.cassandra.meta;

import java.util.List;

/**
 *
 * @author pranab
 */
public class KeySpaceDef {
    private String name;
    private String replicaPlacementStrategy;
    private int replicationFactor;
    private String endPointSnitch;
    private List<ColumnFamilyDef> columnFamilies;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the replicaPlacementStrategy
     */
    public String getReplicaPlacementStrategy() {
        return replicaPlacementStrategy;
    }

    /**
     * @param replicaPlacementStrategy the replicaPlacementStrategy to set
     */
    public void setReplicaPlacementStrategy(String replicaPlacementStrategy) {
        this.replicaPlacementStrategy = replicaPlacementStrategy;
    }

    /**
     * @return the replicationFactor
     */
    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * @param replicationFactor the replicationFactor to set
     */
    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    /**
     * @return the endPointSnitch
     */
    public String getEndPointSnitch() {
        return endPointSnitch;
    }

    /**
     * @param endPointSnitch the endPointSnitch to set
     */
    public void setEndPointSnitch(String endPointSnitch) {
        this.endPointSnitch = endPointSnitch;
    }

    /**
     * @return the columnFamilies
     */
    public List<ColumnFamilyDef> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * @param columnFamilies the columnFamilies to set
     */
    public void setColumnFamilies(List<ColumnFamilyDef> columnFamilies) {
        this.columnFamilies = columnFamilies;
    }
    
    public ColumnFamilyDef findColFamilyByName(String colFam){
        ColumnFamilyDef colFamDef = null;
        for (ColumnFamilyDef thisColFamDef : columnFamilies){
            if (thisColFamDef.getName().equals(colFam)){
                colFamDef = thisColFamDef;
                break;
            }
        }
        
        return colFamDef;
    }

}
