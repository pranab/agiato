/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.connect;

import agiato.cassandra.meta.Host;
import java.util.List;

/**
 *
 * @author pranab
 */
public class RoundRobinLoadBalancer extends LoadBalancer{
    private int current = 0;
    
    public RoundRobinLoadBalancer(){
    }

    
    @Override
    public synchronized HostConnections select() {
        HostConnections selected = hostConnections.get(current);
        current = ++current % hostConnections.size();
        return selected;
    }

}
