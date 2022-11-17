package lab6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)) {

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
