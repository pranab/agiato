/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;


/**
 *
 * @author pranab
 */
public class Util {
    public static final String ENCODING = "utf-8";

    public static byte[] getBytesFromString(String value) throws IOException{
       return value.getBytes(Util.ENCODING);
    }

    public static ByteBuffer getByteBufferFromString(String value) throws IOException{
       return ByteBuffer.wrap(value.getBytes(Util.ENCODING));
    }

    public static String getStringFromBytes(byte[] data) throws IOException{
        return new String(data, ENCODING);
    }

    public static String getStringFromByteBuffer(ByteBuffer data) throws IOException{
        return new String(data.array(), ENCODING);
    }

    public static ByteBuffer getByteBufferFromBytes(byte[] data) throws IOException{
            return ByteBuffer.wrap(data);
    }

    public static byte[] getBytesFromLong(long value) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(value);
        dos.flush();
        return bos.toByteArray();
    }
    
    public static byte[] getBytesFromDouble(double value) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeDouble(value);
        dos.flush();
        return bos.toByteArray();
    }

    public static ByteBuffer getByteBufferFromLong(long value) throws IOException{
        return ByteBuffer.wrap(getBytesFromLong(value));
    }    

    public static ByteBuffer getByteBufferFromDouble(double value) throws IOException{
        return ByteBuffer.wrap(getBytesFromDouble(value));
    }    

    public static Long getLongFromBytes(byte[] data) throws IOException{
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);
        Long value =  dis.readLong();
        return value;
    }

    public static long getLongFromByteBuffer(ByteBuffer data) throws IOException{
        return data.getLong();
    }
    
    public static double getDoubleFromByteBuffer(ByteBuffer data) throws IOException{
        return data.getDouble();
    }

    public static java.util.UUID getTimeUUID(){
        return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

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

}
