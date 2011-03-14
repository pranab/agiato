/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
