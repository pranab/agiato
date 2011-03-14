/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.connect;

import agiato.cassandra.meta.Host;
import org.apache.commons.pool.BasePoolableObjectFactory;

/**
 *
 * @author pranab
 */
public class ConnectionFactory extends BasePoolableObjectFactory{
    private Host host;
    private String keySpace;
    
    public ConnectionFactory(Host host){
        this.host = host;
    }

    @Override
    public Object makeObject() throws Exception {
        Connector connector = new Connector(host);
        if (null != keySpace){
            connector.setKeyspace(keySpace);
        }
        return connector;
    }

    /**
     * @return the keySpace
     */
    public String getKeySpace() {
        return keySpace;
    }

    /**
     * @param keySpace the keySpace to set
     */
    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

}
