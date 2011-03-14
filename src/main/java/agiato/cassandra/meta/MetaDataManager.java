/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.meta;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author pranab
 */
public class MetaDataManager {
    private KeySpaceDef keySpace;
    private Cluster cluster;
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

}
