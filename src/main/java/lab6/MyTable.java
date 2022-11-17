package lab6;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

public class MyTable {
    private final TableName tableName;
    private final Connection connection;

    public MyTable(TableName tableName, Connection connection, String... families) {
        this.tableName = tableName;
        this.connection = connection;
        Tables.createTable(tableName, families);
    }

    public MyTable(String tableName, Connection connection, String... families) {
        this(TableName.valueOf(tableName), connection);
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
}
