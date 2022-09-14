package sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class IcebergHadoopCatalog {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tnv = TableEnvironment.create(settings);
//        tnv.useCatalog("iceberg_meta");

        tnv.executeSql(
                "CREATE CATALOG hadoop_iceberg WITH (" +
                        "'type'='iceberg'," +
                        "'catalog-type'='hadoop'," +
                        "'warehouse'='hdfs://hadoop001:8020/flink_iceberg')");

            tnv.useCatalog("hadoop_iceberg");
        tnv.executeSql("show databases").print();
        tnv.executeSql("use iceberg_db");
        tnv.executeSql("show tables").print();
//        tnv.executeSql(
//                "create table iceberg_db.test(" +
//                        "name string,\n" +
//                        "age int\n" +
//                        ")"
//        );
//        tnv.executeSql("show tables").print();

    }
}
