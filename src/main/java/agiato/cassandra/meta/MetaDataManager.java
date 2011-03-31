/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.meta;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author pranab
 */
public class MetaDataManager {
    private KeySpaceDef keySpace;
    private Cluster cluster;
    private List<IndexDef> indexes;
    private static MetaDataManager metaDataManager;
    
    public static MetaDataManager instance(){
        return metaDataManager;
    }
    
    public static void initialize() throws IOException{
        ObjectMapper mapper = new ObjectMapper(); 
        InputStream in = MetaDataManager.class.getClassLoader().getResourceAsStream("cassandra.json");
        metaDataManager = mapper.readValue(in, MetaDataManager.class);
    }

    /**
     * @return the keySpace
     */
    public KeySpaceDef getKeySpace() {
        return keySpace;
    }

    /**
     * @param keySpace the keySpace to set
     */
    public void setKeySpace(KeySpaceDef keySpace) {
        this.keySpace = keySpace;
    }

    /**
     * @return the cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * @param cluster the cluster to set
     */
    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * @return the indexes
     */
    public List<IndexDef> getIndexes() {
        return indexes;
    }

    /**
     * @param indexes the indexes to set
     */
    public void setIndexes(List<IndexDef> indexes) {
        this.indexes = indexes;
    }
    
    public ColumnFamilyDef findColFamilyByName(String colFamily){
        return keySpace.findColFamilyByName(colFamily);
    }  
    
    public List<IndexDef> findIndexByColFamilyName(String colFamily){
        List<IndexDef> indexList = new ArrayList<IndexDef>();
        for(IndexDef thisIndex : indexes){
            if (thisIndex.getColFamily().equals(colFamily)){
                indexList.add(thisIndex);
            }
        }
        return indexList;
        
    }

}
