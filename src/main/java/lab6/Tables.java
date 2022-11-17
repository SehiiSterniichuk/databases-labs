package lab6;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class Tables {

    public static void dropTable(Connection connection, TableName tableName) {
        try {
            dropTable(connection.getAdmin(), tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropTable(Admin admin, TableName tableName) {
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delete(Connection connection, TableName tableName, byte[] row, byte[] family, byte[] column) {
        Delete delete = new Delete(row);
        delete.addColumn(family, column);
        delete(connection, tableName, delete);
    }

    public static void delete(Connection connection, TableName tableName, byte[] row) {
        Delete delete = new Delete(row);
        delete(connection, tableName, delete);
    }

    public static void delete(Connection connection, TableName tableName, Delete delete) {
        try (var table = connection.getTable(tableName)) {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Result get(Connection connection, TableName tableName, byte[] row) {
        Result result;
        try (var table = connection.getTable(tableName)) {
            Get get = new Get(row);
            result = table.get(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static void put(Connection connection, TableName tableName, String row, String family, String column, String value) {
        put(connection, tableName, row.getBytes(), family.getBytes(), column.getBytes(), value.getBytes());
    }

    public static void put(Connection connection, TableName tableName, byte[] row, byte[] family, byte[] column, byte[] value) {
        try (var table = connection.getTable(tableName)) {
            put(table, row, family, column, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void put(Table table, byte[] row, byte[] family, byte[] column, byte[] value) throws IOException {
        Put p = new Put(row);
        p.addColumn(family, column, value);
        table.put(p);
    }

    public static String scanTable(TableName tableName, Connection connection) {
        try (var table = connection.getTable(tableName)) {
            return scanTable(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String scanTable(Table table) {
        Scan scan = new Scan();
        var builder = new StringBuilder("\n");
        try (var scanner = table.getScanner(scan)) {
            for (var result = scanner.next(); result != null; result = scanner.next()) {
                builder.append(resultToString(result));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return builder.toString();
    }

    public static int count(TableName tableName, Connection connection) {
        try (var table = connection.getTable(tableName)) {
            return count(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int count(Table table) {
        Scan scan = new Scan();
        int counter = 0;
        try (var scanner = table.getScanner(scan)) {
            for (var result = scanner.next(); result != null; result = scanner.next()) {
                counter++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return counter;
    }

    public static StringBuilder resultToString(Result result) {
        var builder = new StringBuilder();
        var row = Bytes.toString(result.getRow());
        List<Cell> cells = result.listCells();
        for (var i : cells) {
            var value = Bytes.toString(i.getValueArray(), i.getValueOffset(), i.getValueLength());
            var column = Bytes.toString(i.getQualifierArray(), i.getQualifierOffset(), i.getQualifierLength());
            var family = Bytes.toString(i.getFamilyArray(), i.getFamilyOffset(), i.getFamilyLength());
            var message = String.format("%s,\tcolumn=%s:%s,\tvalue=%s\n",
                    row, family, column, value);
            builder.append(message);
        }
        return builder;
    }

    public static TableDescriptor createTable(final String tableName, final String... families) {
        return createTable(TableName.valueOf(tableName), families);
    }

    public static TableDescriptor createTable(final TableName tableName, final String... families) {
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        for (var family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family));
            tableBuilder.setColumnFamily(familyDescriptor);
        }
        return tableBuilder.build();
    }


}
