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
public class BaseColumnValue {
     protected ByteBuffer name = Util.getEmptyByteBuffer();;

    /**
     * @return the name
     */
    public ByteBuffer getName() {
        return name;
    }

    public String getNameAsString()  throws IOException{
        return Util.getStringFromByteBuffer(name);
    }

    public long getNameAsLong() throws IOException {
        return Util.getLongFromByteBuffer(name);
    }

    /**
     * @param name the name to set
     */
    public void setName(ByteBuffer name) {
        this.name = name;
    }

    public void setNameFromString(String name) throws IOException {
        this.name = Util.getByteBufferFromString(name) ;
    }

    public void setNameFromLong(long name) throws IOException {
        this.name = Util.getByteBufferFromLong(name);
    }


}
