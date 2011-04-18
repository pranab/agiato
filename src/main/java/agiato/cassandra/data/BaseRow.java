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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author pranab
 */
public class BaseRow {
    private ByteBuffer key;

    /**
     * @return the key
     */
    public ByteBuffer getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(ByteBuffer key) {
        this.key = key;
    }

    /**
     * @param key the key to set
     */
    public void setLongKey(long key) throws IOException {
        this.key = Util.getByteBufferFromLong(key);
    }

    /**
     * @param key the key to set
     */
    public void setStringKey(String key) throws IOException {
        this.key = Util.getByteBufferFromString(key);
    }
}
