/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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

}
