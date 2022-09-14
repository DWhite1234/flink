package sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class IcebergHiveCatalog {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tnv = TableEnvironment.create(settings);
//        tnv.useCatalog("iceberg_meta");

        tnv.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://hadoop001:9083',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs://hadoop001:8020/hive_catalog'\n" +
                ");");
        tnv.executeSql("show databases").print();

    }
}
