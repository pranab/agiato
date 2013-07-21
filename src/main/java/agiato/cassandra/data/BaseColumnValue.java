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
 * Base class for column value
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

    /**
     * @return
     * @throws IOException
     */
    public long getNameAsLong() throws IOException {
        return Util.getLongFromByteBuffer(name);
    }

    /**
     * @param name the name to set
     */
    public void setName(ByteBuffer name) {
        this.name = name;
    }

    /**
     * @param name
     * @throws IOException
     */
    public void setNameFromString(String name) throws IOException {
        this.name = Util.getByteBufferFromString(name) ;
    }

    /**
     * @param name
     * @throws IOException
     */
    public void setNameFromLong(long name) throws IOException {
        this.name = Util.getByteBufferFromLong(name);
    }


}
