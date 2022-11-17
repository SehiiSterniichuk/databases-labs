package lab6;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class MyTable {
    private final TableName tableName;
    private final Connection connection;
    private final Admin admin;

    public MyTable(TableName tableName, Connection connection) {
        this.tableName = tableName;
        this.connection = connection;
        try {
            this.admin = connection.getAdmin();
            if(!admin.tableExists(tableName)){

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MyTable(String tableName, Connection connection) {
        this(TableName.valueOf(tableName), connection);
    }


}
