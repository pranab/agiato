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

import agiato.cassandra.connect.Connector;
import agiato.cassandra.connect.HostConnections;
import agiato.cassandra.connect.LoadBalancer;
import agiato.cassandra.meta.Cluster;
import agiato.cassandra.meta.ColumnFamilyDef;
import agiato.cassandra.meta.Host;
import agiato.cassandra.meta.IndexDef;
import agiato.cassandra.meta.MetaDataManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 *
 * @author pranab
 */
public class DataManager {
    //private static GenericObjectPool connectionPool;
    private LoadBalancer loadBalancer;
    private static DataManager dataManager;
    
    public static synchronized DataManager initialize(String configFile, boolean forceCreateKeySpace) throws Exception{
        if (null == dataManager){
            dataManager = new DataManager(configFile, forceCreateKeySpace);
        }
        return dataManager;
    }
    
    
    public static DataManager instance(){
        return dataManager;
    }
            
    public DataManager(String configFile, boolean forceCreateKeySpace) throws Exception{
        MetaDataManager.initialize(configFile);
        MetaDataManager metaDatamanager = MetaDataManager.instance();
        Cluster cluster = metaDatamanager.getCluster();

        List<HostConnections> hostConnections = new ArrayList<HostConnections>();
        for(Host host : cluster.getHosts()){
            hostConnections.add(new HostConnections(host, cluster.getPoolConfig()));
        }
        
        //load balancer
        String loadBalancerClass = cluster.getLoadBalancerClass();
        Class<? extends LoadBalancer> clazz = Class.forName(loadBalancerClass).asSubclass(LoadBalancer.class);
        loadBalancer = clazz.newInstance();
        loadBalancer.setHostConnections(hostConnections);
        
        GenericObjectPool connectionPool = getConnectionPool();    
        Connector connector = (Connector)connectionPool.borrowObject();
        Cassandra.Client client = connector.openConnection();
        String keySpaceName = metaDatamanager.getKeySpace().getName();

        try{
            List<KsDef> ksDefs = client.describe_keyspaces();
            boolean exists = false;
            for (KsDef ksDef : ksDefs){
                if (ksDef.getName().equals(keySpaceName)){
                    exists = true;
                    break;
                }
            }
            
            //drop existing key space
            if (exists && forceCreateKeySpace){
                client.system_drop_keyspace(keySpaceName);
                exists = false;
            }

            //create keyspace if it does not exist
            if (!exists){
                KsDef ks = new KsDef();
                ks.setName(keySpaceName);
                ks.setReplication_factor(metaDatamanager.getKeySpace().getReplicationFactor());
                ks.setStrategy_class(metaDatamanager.getKeySpace().getReplicaPlacementStrategy());

                List<CfDef> cfDefs = new ArrayList<CfDef>();
                for (ColumnFamilyDef colFam : metaDatamanager.getKeySpace().getColumnFamilies()) {
                    CfDef cfDef = new CfDef();
                    cfDef.setKeyspace(metaDatamanager.getKeySpace().getName());
                    cfDef.setName(colFam.getName());

                    cfDef.setColumn_type(colFam.getColumnType());
                    cfDef.setComparator_type(colFam.getCompareWith());
                    if (colFam.isSuperColumn()){
                        cfDef.setSubcomparator_type(colFam.getCompareSubcolumnsWith());
                    }
                    if(colFam.isKeysCachedSet()){
                      cfDef.setKey_cache_size(colFam.getKeysCached());
                    }
                    if (colFam.isRowsCachedSet()){
                        cfDef.setRow_cache_size(colFam.getRowsCached());
                    }

                    cfDefs.add(cfDef);

                }

                ks.setCf_defs(cfDefs);
                client.system_add_keyspace(ks);
                System.out.println("keyspace created : " + keySpaceName);
            } else {
                System.out.println("keyspace already exists : " + keySpaceName);
            }
            
            //create cf for indexes
            KsDef ksDef = client.describe_keyspace(keySpaceName);
            Set<String> colFamSet = new HashSet<String>();
            if (null != metaDatamanager.getIndexes()) {
	            for (IndexDef indexDef : metaDatamanager.getIndexes()){
	                String cfName = indexDef.getColFamilyName();
	                List<CfDef> cfDefs = ksDef.getCf_defs();
	                boolean cfExists = false;
	
	                for (CfDef cfDef : cfDefs){
	                    if (cfDef.getName().equals(cfName)) {
	                        cfExists = true;
	                        break;
	                    }
	                }
	
	                String colFamName = indexDef.getColFamilyName();
	                if (!cfExists && !colFamSet.contains(colFamName)){
	                    CfDef cfDef = new CfDef();
	                    cfDef.setKeyspace(keySpaceName);
	                    cfDef.setName(colFamName);
	                    boolean isSuperCol = indexDef.isCatIndexName();
	                    cfDef.setColumn_type(isSuperCol? "Super" : "Standard");
	                    cfDef.setComparator_type(indexDef.getIndexedDataType() == IndexDef.TYPE_STRING? "UTF8Type" : "LongType");
	                    if (isSuperCol){
	                        cfDef.setSubcomparator_type(indexDef.getStoredDataType() == IndexDef.TYPE_STRING? "UTF8Type" : "LongType");
	                    }
	                    cfDef.setKey_cache_size(indexDef.getIndexKeysCached());
	                    cfDef.setRow_cache_size(indexDef.getIndexRowsCached());
	                    
	                    client.system_add_column_family(cfDef);
	                    colFamSet.add(colFamName);
	
	                }
	            }
            }
            
            
            //asociate key space 
            loadBalancer.associateKeySpace(keySpaceName);
            
        } finally {
            client.set_keyspace(keySpaceName);
            returnConnection(connector);
        }

    }
    
    public Connector  borrowConnection() throws Exception{
        HostConnections hostConns = loadBalancer.select();
        return (Connector)hostConns.getConnectionPool().borrowObject();
    }
    
    public Connector  borrowConnection(GenericObjectPool connectionPool) throws Exception{
        return (Connector)connectionPool.borrowObject();
    }

    public void returnConnection(GenericObjectPool connectionPool, Connector connector) 
        throws Exception{
        connectionPool.returnObject(connector);
    }
    
    public void returnConnection(Connector connector) 
        throws Exception{
        GenericObjectPool connectionPool = loadBalancer.findConnectionPool(connector.getHost());
        connectionPool.returnObject(connector);
    }

    public GenericObjectPool getConnectionPool(){
        return loadBalancer.select().getConnectionPool();
    }
    
}
