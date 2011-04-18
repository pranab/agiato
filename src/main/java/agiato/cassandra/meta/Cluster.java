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
