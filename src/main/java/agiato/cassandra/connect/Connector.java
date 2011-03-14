/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.connect;

import agiato.cassandra.meta.Host;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport;

/**
 *
 * @author pranab
 */
public class Connector {
    //private  TTransport trans = null;
    private  TFramedTransport trans = null;
    private  Cassandra.Client client = null;
    private Host host;
    
    private static Connector connector = new Connector();

    public static Connector instance() {
        return connector;
    }
    
    public Connector(){
        
    }
    
    public Connector(Host host){
       this.host = host; 
       System.out.println("Connection created for host: " + host.getName());
    }

    public  Cassandra.Client openConnection() {
        try {
            if (null == trans){
                //trans = new TSocket("localhost", 9160);
                trans = new TFramedTransport(new TSocket(getHost().getName(), getHost().getPort()));
                
                TProtocol proto = new TBinaryProtocol(trans);
                client = new Cassandra.Client(proto);
                trans.open();
            }
        } catch (TTransportException exception) {
            System.out.println("failed to open Cassandra connection" + exception);
            exception.printStackTrace();
        }

        return client;
    }

    public  void closeConnection() {
        try {
            trans.flush();
            trans.close();
            trans = null;
        } catch (TTransportException exception) {
            System.out.println("failed to close Cassandra connection");
            exception.printStackTrace();
        }
    }

    public void setKeyspace(String keyspace){
        try {
            openConnection().set_keyspace(keyspace);
        } catch (InvalidRequestException ex) {
            System.out.println("failed to set keyspace in Cassandra connection");
            ex.printStackTrace();
        } catch (TException ex) {
            System.out.println("failed to set keyspace in Cassandra connection");
            ex.printStackTrace();
        }
        
    }

    /**
     * @return the host
     */
    public Host getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(Host host) {
        this.host = host;
    }
}
