package lab6;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Tables {

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

    public static String scan(TableName tableName, Connection connection) {
        try (var table = connection.getTable(tableName)) {
            return scan(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String scan(Table table) {
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

    public static boolean exists(Connection connection, TableName tableName){
        try {
            return connection.getAdmin().tableExists(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void drop(Connection connection, TableName tableName) {
        try {
            drop(connection.getAdmin(), tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void drop(Admin admin, TableName tableName) {
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public static List<String> getListOfRow(TableName tableName, Connection connection) {
        try (var table = connection.getTable(tableName)) {
            return getListOfRow(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getListOfRow(Table table){
        Scan scan = new Scan();
        List<String> rows = new ArrayList<>();
        try (var scanner = table.getScanner(scan)) {
            for (var result = scanner.next(); result != null; result = scanner.next()) {
                rows.add(Bytes.toString(result.getRow()));
            }
            return rows;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public static TableDescriptor createTableDescriptor(final TableName tableName, final String... families) {
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        return createTableDescriptor(tableBuilder, families);
    }

    public static TableDescriptor createTableDescriptor(TableDescriptorBuilder tableBuilder, final String... families){
        if(families.length == 0){
            throw new IllegalArgumentException("Table must have at least one column family! Your family list is empty.");
        }
        for (var family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family));
            tableBuilder.setColumnFamily(familyDescriptor);
        }
        return tableBuilder.build();
    }
}
