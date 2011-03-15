/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.data;

import agiato.cassandra.connect.Connector;
import agiato.cassandra.connect.HostConnections;
import agiato.cassandra.connect.LoadBalancer;
import agiato.cassandra.meta.Cluster;
import agiato.cassandra.meta.ColumnFamilyDef;
import agiato.cassandra.meta.Host;
import agiato.cassandra.meta.MetaDataManager;
import java.util.ArrayList;
import java.util.List;
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
    
    public static synchronized DataManager initialize(boolean forceCreateKeySpace) throws Exception{
        if (null == dataManager){
            dataManager = new DataManager(forceCreateKeySpace);
        }
        return dataManager;
    }
    
    
    public static DataManager instance(){
        return dataManager;
    }
            
    public DataManager(boolean forceCreateKeySpace) throws Exception{
        MetaDataManager.initialize();
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
