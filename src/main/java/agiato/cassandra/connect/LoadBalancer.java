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
