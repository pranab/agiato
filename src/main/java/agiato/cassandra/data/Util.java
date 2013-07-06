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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;


/**
 * byte encoding decoding
 * @author pranab
 */
public class Util {
    public static final String ENCODING = "utf-8";

    /**
     * @param value
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromString(String value) throws IOException{
       return value.getBytes(Util.ENCODING);
    }

    /**
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromString(String value) throws IOException{
       return ByteBuffer.wrap(value.getBytes(Util.ENCODING));
    }


    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromBytes(byte[] data) throws IOException{
            return ByteBuffer.wrap(data);
    }

    /**
     * @param value
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromLong(long value) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(value);
        dos.flush();
        return bos.toByteArray();
    }
    
    /**
     * @param value
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromDouble(double value) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeDouble(value);
        dos.flush();
        return bos.toByteArray();
    }

    /**
     * @param values
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromList(List<Object> values) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        for (Object value : values){
            if (value instanceof String)
                dos.writeUTF((String)value);
            else if (value instanceof Long)
                dos.writeLong((Long)value);
            else if (value instanceof Double)
                dos.writeDouble((Long)value);
            
        }
        dos.flush();
        return bos.toByteArray();
    }
    
    /**
     * @param values
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromList(List<Object> values) throws IOException{
        return ByteBuffer.wrap(getBytesFromList(values));
    }
    
    /**
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromLong(long value) throws IOException{
        return ByteBuffer.wrap(getBytesFromLong(value));
    }    

    /**
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromDouble(double value) throws IOException{
        return ByteBuffer.wrap(getBytesFromDouble(value));
    }    
    
    /**
     * @param uuid
     * @return
     */
    public static byte[] getBytesFromUUID(java.util.UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[16];

        for (int i = 0; i < 8; i++) {
                buffer[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++) {
                buffer[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return buffer;
    }
    
    /**
     * @param uuid
     * @return
     */
    public static ByteBuffer getByteBufferFromUUID(java.util.UUID uuid) {
        return ByteBuffer.wrap(getBytesFromUUID(uuid));
    }    
    

    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static double getDoubleFromByteBuffer(ByteBuffer data) throws IOException{
        return getDoubleFromBytes(data.array());
    }

    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static String getStringFromBytes(byte[] data) throws IOException{
        return new String(data, ENCODING);
    }

    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static String getStringFromByteBuffer(ByteBuffer data) throws IOException{
        return new String(data.array(), ENCODING);
    }
    
    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static Long getLongFromBytes(byte[] data) throws IOException{
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);
        Long value =  dis.readLong();
        return value;
    }
    
    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static Double getDoubleFromBytes(byte[] data) throws IOException{
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);
        Double value =  dis.readDouble();
        return value;
    }

    /**
     * @param data
     * @return
     * @throws IOException
     */
    public static Long getLongFromByteBuffer(ByteBuffer data) throws IOException{
        return getLongFromBytes(data.array());
    }

    /**
     * @param data
     * @param classes
     * @return
     * @throws IOException
     */
    public static List<Object>  getListFromBytes(byte[] data, List<Class> classes) throws IOException{
        List<Object> values = new ArrayList<Object>();
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);
        for (Class cl : classes){
            Object value = null;
            if (cl == String.class)
                value =  dis.readUTF();
            else if (cl == Long.class)
                value =  dis.readLong();
            else if (cl == Double.class)
                value =  dis.readDouble();
            
            if (null != value)
                values.add(value);
        }
        dis.close();
        return values;
    }
    
    /**
     * @param data
     * @param classes
     * @return
     * @throws IOException
     */
    public static List<Object>  getListFromByteBuffer(ByteBuffer data, List<Class> classes) throws IOException{
        return getListFromBytes(data.array(), classes);
    }
    
    /**
     * @return
     */
    public static java.util.UUID getTimeUUID(){
        return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

    /**
     * @param uuid
     * @return
     */
    public static java.util.UUID getUUIDFromBytes( byte[] uuid ){
        long msb = 0;
        long lsb = 0;
        assert uuid.length == 16;
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (uuid[i] & 0xff);
        for (int i=8; i<16; i++)
            lsb = (lsb << 8) | (uuid[i] & 0xff);

        com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb,lsb);
        return java.util.UUID.fromString(u.toString());
    }

    /**
     * @param uuid
     * @return
     */
    public static java.util.UUID getUUIDFromByteBuffer(ByteBuffer uuid ){
        return getUUIDFromBytes(uuid.array());
    }
    
    /**
     * @param cList
     * @return
     * @throws Exception
     */
    public static  Map<String, String> getColumns( List<ColumnOrSuperColumn> cList) throws Exception{
        Map<String, String> columns = new HashMap<String, String>();

        for (ColumnOrSuperColumn colSup : cList){
            Column col = colSup.getColumn();
            if (null != col){
                String name = getStringFromBytes(col.getName());
                String value = getStringFromBytes(col.getValue());
                columns.put(name, value);
            }
        }
        return columns;
    }
    
    /**
     * @return
     */
    public static ByteBuffer getEmptyByteBuffer(){
        byte[] data = {};
        return ByteBuffer.wrap(data);
    }

    public static byte[] getBytesFromObject(Object obj) throws IOException {
    	byte[] bytes = null;
		if (obj instanceof String) {
			bytes = getBytesFromString((String)obj);
		} else if (obj instanceof Long) {
			bytes = getBytesFromLong((Long)obj);
		} else if (obj instanceof Double) {
			bytes = getBytesFromDouble((Double)obj);
		}
		return bytes;
    }
    /**
     * @param bytesList
     * @return
     */
    public static  byte[] makeComposite(List<byte[]> bytesList){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (byte[] bytes : bytesList){
        	//length
        	bos.write( (byte) ((bytes.length >> 8 ) & 0xFF)) ;
        	bos.write( (byte) (bytes.length & 0xFF)) ;
          
        	//content
        	for (int j=0;j<bytes.length;j++){
        		bos.write( bytes[j] & 0xFF) ;
        	}
        	
        	//sentinel
        	bos.write((byte)0);
        }
        return bos.toByteArray();
      }   
}