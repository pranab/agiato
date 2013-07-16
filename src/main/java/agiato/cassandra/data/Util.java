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
     * byte array from string
     * @param value
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromString(String value) throws IOException{
       return value.getBytes(Util.ENCODING);
    }

    /**
     * ByteBuffer from String
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromString(String value) throws IOException{
       return ByteBuffer.wrap(value.getBytes(Util.ENCODING));
    }


    /**
     * ByteBuffer from byte array
     * @param data
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromBytes(byte[] data) throws IOException{
            return ByteBuffer.wrap(data);
    }

    /**
     * byte array from long
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
     * byte array from long
     * @param value
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromInt(int value) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(value);
        dos.flush();
        return bos.toByteArray();
    }

    /**
     * byte array from double
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
     * byte array from list
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
            else if (value instanceof Integer)
                dos.writeInt((Integer)value);
            else if (value instanceof Double)
                dos.writeDouble((Long)value);
            
        }
        dos.flush();
        return bos.toByteArray();
    }
    
    /**
     * ByteBuffer from list
     * @param values
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromList(List<Object> values) throws IOException{
        return ByteBuffer.wrap(getBytesFromList(values));
    }
    
    /**
     * ByteBuffer from long
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromLong(long value) throws IOException{
        return ByteBuffer.wrap(getBytesFromLong(value));
    }    

    /**
     * ByteBuffer from long
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromInt(int value) throws IOException{
        return ByteBuffer.wrap(getBytesFromInt(value));
    }    

    /**
     * ByteBuffer from double
     * @param value
     * @return
     * @throws IOException
     */
    public static ByteBuffer getByteBufferFromDouble(double value) throws IOException{
        return ByteBuffer.wrap(getBytesFromDouble(value));
    }    
    
    /**
     * byte array from UUID
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
     * ByteBuffer from UUID
     * @param uuid
     * @return
     */
    public static ByteBuffer getByteBufferFromUUID(java.util.UUID uuid) {
        return ByteBuffer.wrap(getBytesFromUUID(uuid));
    }    
 
    /**
     * double from ByteBuffer
     * @param data
     * @return
     * @throws IOException
     */
    public static double getDoubleFromByteBuffer(ByteBuffer data) throws IOException{
        return getDoubleFromBytes(data.array());
    }

    /**
     * string from byte array
     * @param data
     * @return
     * @throws IOException
     */
    public static String getStringFromBytes(byte[] data) throws IOException{
        return new String(data, ENCODING);
    }

    /**
     * string from ByteBuffer
     * @param data
     * @return
     * @throws IOException
     */
    public static String getStringFromByteBuffer(ByteBuffer data) throws IOException{
        return new String(data.array(), ENCODING);
    }
    
    /**
     * long from  byte array
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
     * long from  byte array
     * @param data
     * @return
     * @throws IOException
     */
    public static Integer getIntFromBytes(byte[] data) throws IOException{
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);
        Integer value =  dis.readInt();
        return value;
    }

    /**
     * double from byte array
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
     * long from ByteBuffer
     * @param data
     * @return
     * @throws IOException
     */
    public static Long getLongFromByteBuffer(ByteBuffer data) throws IOException{
        return getLongFromBytes(data.array());
    }

    /**
     * int from ByteBuffer
     * @param data
     * @return
     * @throws IOException
     */
    public static Integer getIntFromByteBuffer(ByteBuffer data) throws IOException{
        return getIntFromBytes(data.array());
    }

    /**
     * list from byte array
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
            else if (cl == Integer.class)
                value =  dis.readInt();
            else if (cl == Double.class)
                value =  dis.readDouble();
            
            if (null != value)
                values.add(value);
        }
        dis.close();
        return values;
    }
    
    /**
     * list from ByteBuffer
     * @param data
     * @param classes
     * @return
     * @throws IOException
     */
    public static List<Object>  getListFromByteBuffer(ByteBuffer data, List<Class> classes) throws IOException{
        return getListFromBytes(data.array(), classes);
    }
    
    /**
     * time UUID
     * @return
     */
    public static java.util.UUID getTimeUUID(){
        return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

    /**
     * UUID from byte array
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
     * UUID from ByteBuffer
     * @param uuid
     * @return
     */
    public static java.util.UUID getUUIDFromByteBuffer(ByteBuffer uuid ){
        return getUUIDFromBytes(uuid.array());
    }
    
    /**
     * map from list of columns
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
     * empty ByteBuffer
     * @return
     */
    public static ByteBuffer getEmptyByteBuffer(){
        byte[] data = {};
        return ByteBuffer.wrap(data);
    }

    /**
     * byte arrayfrom primitives
     * @param obj
     * @return
     * @throws IOException
     */
    public static byte[] getBytesFromObject(Object obj) throws IOException {
    	byte[] bytes = null;
		if (obj instanceof String) {
			bytes = getBytesFromString((String)obj);
		} else if (obj instanceof Long) {
			bytes = getBytesFromLong((Long)obj);
		} else if (obj instanceof Integer) {
			bytes = getBytesFromInt((Integer)obj);
		} else if (obj instanceof Double) {
			bytes = getBytesFromDouble((Double)obj);
		} else if (obj instanceof List<?>) {
			StringBuilder stBld = new  StringBuilder("[");
			for (Object elem : (List<?>)obj) {
				stBld.append(elem).append(",");
			}
			stBld.deleteCharAt(stBld.length()-1);
			stBld.append("]");
			bytes = getBytesFromString(stBld.toString());
		} else if (obj instanceof Map<?, ?>) {
			StringBuilder stBld = new  StringBuilder("{");
			Map<?,?> map = (Map<?,?>)obj;
			for (Object elem : map.entrySet()) {
				Map.Entry<?,?> entry = (Map.Entry<?,?>)elem;
				stBld.append(entry.getKey()).append(":").append(entry.getValue());
			}
			stBld.deleteCharAt(stBld.length()-1);
			stBld.append("}");
			bytes = getBytesFromString(stBld.toString());
		}
		return bytes;
    }
    
    /**
     * makes composite column name or row key
     * @param bytesList
     * @return
     */
    public static  byte[] encodeComposite(List<byte[]> bytesList){
    	byte[] encBytes;
    	if (bytesList.size() == 1) {
    		encBytes = bytesList.get(0);
    	} else  {
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
	        encBytes =  bos.toByteArray();
    	}
    	return encBytes;
      }   

    /**
     * desrializes encoded composite key
     * @param encBytes
     * @return
     */
    public static  List<byte[]>  dcodeComposite(byte[] encBytes) {
    	List<byte[]> bytesList = new ArrayList<byte[]>();
    	int cur = 0;
    	int elemLen = 0;
    	byte[] elemBytes = null;
    	while (cur  < encBytes.length) {
    		//length
    		elemLen = ( encBytes[cur++] & 0xFF) << 8;
    		elemLen = elemLen | (encBytes[cur++] & 0xFF);
    		elemBytes = new byte[elemLen];
    		
    		//copy element
    		System.arraycopy(encBytes, cur,  elemBytes,  0, elemLen);
    		bytesList.add(elemBytes);
    		
    		//reposition
    		cur += elemLen + 1;
    	}
    	return bytesList;
    }
    
}