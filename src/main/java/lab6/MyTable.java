package lab6;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;

public class MyTable {
    private final TableName tableName;
    private final Connection connection;

    public MyTable(Connection connection, TableName tableName, String... families) {
        this.tableName = tableName;
        this.connection = connection;
//        TableDescriptor tableDescriptor = Tables.createTableDescriptor(tableName);
        TableDescriptor tableDescriptor = Tables.addFamilies(tableName, families);
        try {
            Admin admin = connection.getAdmin();
            if(!admin.tableExists(tableName)){
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MyTable(Connection connection, String tableName, String... families) {
        this(connection, TableName.valueOf(tableName), families);
    }

    public String scan() {
        return Tables.scanTable(tableName, connection);
    }

    public void put(String row, String family, String column, String value) {
        Tables.put(connection, tableName, row, family, column, value);
    }

    public String get(String row) {
        return Tables.resultToString(Tables.get(connection, tableName, row.getBytes())).toString();
    }

    public void delete(String row) {
        Tables.delete(connection, tableName, row.getBytes());
    }

    public void delete(String row, String family, String column) {
        Tables.delete(connection, tableName, row.getBytes(), family.getBytes(), column.getBytes());
    }

    public void drop() {
        Tables.dropTable(connection, tableName);
    }

    public int count() {
        return Tables.count(tableName, connection);
    }
}
