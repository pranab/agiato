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
