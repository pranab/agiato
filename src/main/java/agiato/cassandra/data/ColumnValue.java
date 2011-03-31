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
public class ColumnValue  extends BaseColumnValue{
     private ByteBuffer value = Util.getEmptyByteBuffer();

    public void write(String name, String value) throws IOException{
        this.name = Util.getByteBufferFromString(name) ;
        this.value = Util.getByteBufferFromString(value) ;
    }

    public void write(String name, long value) throws IOException{
        this.name = Util.getByteBufferFromString(name) ;
        this.value = Util.getByteBufferFromLong(value);
    }

    public void write(String name, double value) throws IOException{
        this.name = Util.getByteBufferFromString(name) ;
        this.value = Util.getByteBufferFromDouble(value);
    }

    public String[] read() throws IOException{
        String[] nameValue = new String[2];
        nameValue[0] = Util.getStringFromByteBuffer(name);
        nameValue[1] = Util.getStringFromByteBuffer(value);
        return nameValue;
    }

    /**
     * @return the value
     */
    public ByteBuffer getValue() {
        return value;
    }

    public String getValueAsString()  throws IOException {
        return Util.getStringFromByteBuffer(value);
    }

    public long getValueAsLong() throws IOException {
        return Util.getLongFromByteBuffer(value);
    }

    public double getValueAsDouble() throws IOException {
        return Util.getDoubleFromByteBuffer(value);
    }

    /**
     * @param value the value to set
     */
    public void setValue(ByteBuffer value) {
        this.value = value;
    }

     public void setValueFromString(String value) throws IOException {
        this.value = Util.getByteBufferFromString(value);
    }


    public void setValueFromLong(long value) throws IOException {
        this.value = Util.getByteBufferFromLong(value);
    }

}
