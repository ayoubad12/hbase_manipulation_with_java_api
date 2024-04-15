package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class App {
    public static final String TABLE_NAME = "users";
    public static final String CF_PERSONAL_DATA = "personal_data";
    public static final String CF_PROFESSIONAL_DATA = "professional_data";

    public static void main(String[] args) {
        // HBase Configuration
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "hbase-master:16000");

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL_DATA));
            TableDescriptor tableDescriptor = builder.build();

            if (!admin.tableExists(tableName)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created");
            } else {
                System.out.println("Table already exists");
            }

            // Working with data
            try (Table table = connection.getTable(tableName)) {
                // Insert data
                Put put = new Put("1".getBytes());
                put.addColumn(CF_PERSONAL_DATA.getBytes(), "name".getBytes(), "John".getBytes());
                put.addColumn(CF_PERSONAL_DATA.getBytes(), "surname".getBytes(), "Doe".getBytes());
                put.addColumn(CF_PROFESSIONAL_DATA.getBytes(), "position".getBytes(), "Software Engineer".getBytes());
                put.addColumn(CF_PROFESSIONAL_DATA.getBytes(), "salary".getBytes(), "100000".getBytes());
                table.put(put);
                System.out.println("Data inserted");

                // Retrieve data
                Get get = new Get("1".getBytes());
                Result result = table.get(get);
                System.out.println("Get result: " + result);

                // Delete data
                Delete delete = new Delete("1".getBytes());
                table.delete(delete);
                System.out.println("Data deleted");
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Delete table if it exists
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("Table deleted");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}