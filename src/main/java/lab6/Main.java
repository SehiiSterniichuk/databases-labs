package lab6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
/*
* Встановіть бібліотеку мови програмування на ваш вибір і реалізуйте
функції:
• створення тестових двох таблиць і двох сімейств стовпців;
• дозаписування даних у дві таблиці;
• зчитування даних з двох таблиць;
• оновлення даних у двох таблицях;
• видалення даних з двох таблиць;
• видалення таблиць.
*
* */
import java.io.IOException;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        TableName table1 = TableName.valueOf("worker");


        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
//            if (admin.tableExists(table1)) {
//                admin.disableTable(table1);
//                admin.deleteTable(table1);
//            }
//            var descriptor = createTable(table1, family1, family2);
//            admin.createTable(descriptor);
            var scanResult = scanTable(table1, connection);
            System.out.println(scanResult);

        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }

    private static void put(TableName tableName, String row, List<Cell> cells, Connection connection) {
        try (var table = connection.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes(row));
            for (var cell : cells) {
                p.add(cell);
            }
            table.put(p);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String scanTable(TableName tableName, Connection connection) {
        try (var table = connection.getTable(tableName)) {
            return scanTable(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String scanTable(Table table) {
        Scan scan = new Scan();
        var builder = new StringBuilder("\n");
        try (var scanner = table.getScanner(scan)) {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                var row = Bytes.toString(result.getRow());
                List<Cell> cells = result.listCells();
                for (var i : cells) {
                    var value = Bytes.toString(i.getValueArray(), i.getValueOffset(), i.getValueLength());
                    var column = Bytes.toString(i.getQualifierArray(), i.getQualifierOffset(), i.getQualifierLength());
                    var family = Bytes.toString(i.getFamilyArray(), i.getFamilyOffset(), i.getFamilyLength());
                    var message = String.format("Found row : %s,\tcolumn=%s:%s,\tvalue=%s\n",
                            row, family, column, value);
                    builder.append(message);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return builder.toString();
    }

    private static TableDescriptor createTable(final String tableName, final String... families) {
        return createTable(TableName.valueOf(tableName), families);
    }

    private static TableDescriptor createTable(final TableName tableName, final String... families) {
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        for (var family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family));
            tableBuilder.setColumnFamily(familyDescriptor);
        }
        return tableBuilder.build();
    }
}
