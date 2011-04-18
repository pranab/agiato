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
    private List<Query> queries;
    
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

    /**
     * @return the queries
     */
    public List<Query> getQueries() {
        return queries;
    }

    /**
     * @param queries the queries to set
     */
    public void setQueries(List<Query> queries) {
        this.queries = queries;
    }
    
    public Query findQuery(String name){
        Query query = null;
        
        return query;
    }

}
