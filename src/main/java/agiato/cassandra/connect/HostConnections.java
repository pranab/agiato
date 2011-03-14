/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.connect;

import agiato.cassandra.meta.Host;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 *
 * @author pranab
 */
public class HostConnections {
    private GenericObjectPool connectionPool;
    private Host host;
    private ConnectionFactory connectionFactory;
    
    public  HostConnections(Host host, GenericObjectPool.Config poolConfig){
        this.host = host;
        connectionFactory = new ConnectionFactory(host);
        connectionPool = new GenericObjectPool(connectionFactory, poolConfig);
    }

    /**
     * @return the connectionPool
     */
    public GenericObjectPool getConnectionPool() {
        return connectionPool;
    }

    /**
     * @param connectionPool the connectionPool to set
     */
    public void setConnectionPool(GenericObjectPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    /**
     * @return the host
     */
    public Host getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(Host host) {
        this.host = host;
    }

    /**
     * @return the connectionFactory
     */
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * @param connectionFactory the connectionFactory to set
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }


}
