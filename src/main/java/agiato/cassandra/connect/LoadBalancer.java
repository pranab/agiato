/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.connect;

import agiato.cassandra.meta.Host;
import java.util.List;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 *
 * @author pranab
 */
public abstract class LoadBalancer {
    protected List<HostConnections> hostConnections;

    public LoadBalancer(){
    }
    


    public abstract HostConnections select();

    /**
     * @return the hostConnections
     */
    public List<HostConnections> getHostConnections() {
        return hostConnections;
    }

    /**
     * @param hostConnections the hostConnections to set
     */
    public void setHostConnections(List<HostConnections> hostConnections) {
        this.hostConnections = hostConnections;
    }
    
    public GenericObjectPool findConnectionPool(Host host){
        GenericObjectPool connPool = null;
        for (HostConnections hostConn : hostConnections){
            if (hostConn.getHost() == host){
                connPool = hostConn.getConnectionPool();
                break;
            }
        }
        
        return connPool;
    }
    
    public void associateKeySpace(String keySpace){
        for (HostConnections hostConn : hostConnections){
            hostConn.getConnectionFactory().setKeySpace(keySpace);
        }        
    }
}
