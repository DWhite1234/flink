package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tnv = StreamTableEnvironment.create(env);

//        TableConfig config = tnv.getConfig();
//        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
//        config.getConfiguration().setString("table.exec.emit.early-fire.delay","1s");
        tnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `name` String,\n" +
                "  `age` int,\n" +
//                "  `ts` as date_format(PROCTIME(),'yyyy-MM-dd HH:mm:ss')" +
                "  `ts` as PROCTIME()" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'demo',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'" +
                ")");

        //根据性别分组,分别统计分,天,月
        Table table = tnv.sqlQuery("select window_start,window_end,count(1) " +
                "from TABLE(" +
                "CUMULATE(Table KafkaTable, DESCRIPTOR(ts), INTERVAL '1' SECONDS,INTERVAL '1' MINUTES)" +
                ") " +
                "group by window_start,window_end");

        tnv.createTemporaryView("tmp",table);
        tnv.executeSql("select * from tmp").print();

    }
}
