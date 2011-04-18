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
