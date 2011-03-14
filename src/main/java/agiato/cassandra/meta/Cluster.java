/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.meta;

import java.util.List;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 *
 * @author pranab
 */
public class Cluster {
    private List<Host> hosts;
    private String loadBalancerClass;
    private GenericObjectPool.Config poolConfig;

    /**
     * @return the hosts
     */
    public List<Host> getHosts() {
        return hosts;
    }

    /**
     * @param hosts the hosts to set
     */
    public void setHosts(List<Host> hosts) {
        this.hosts = hosts;
    }

    /**
     * @return the loadBalancerClass
     */
    public String getLoadBalancerClass() {
        return loadBalancerClass;
    }

    /**
     * @param loadBalancerClass the loadBalancerClass to set
     */
    public void setLoadBalancerClass(String loadBalancerClass) {
        this.loadBalancerClass = loadBalancerClass;
    }

    /**
     * @return the poolConfig
     */
    public GenericObjectPool.Config getPoolConfig() {
        return poolConfig;
    }

    /**
     * @param poolConfig the poolConfig to set
     */
    public void setPoolConfig(GenericObjectPool.Config poolConfig) {
        this.poolConfig = poolConfig;
    }
    

}
