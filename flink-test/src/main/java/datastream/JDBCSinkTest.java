package datastream;

import bean.Config;
import bean.TestBean;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class JDBCSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        int a = 0;

        /**
         * jdbcSink:
         *  update,insert,delete 都支持
         *
         *  在数据本身中插入sql,不行,会报错
         */
        env.socketTextStream("localhost", 999)
                .map(data -> {
                    return JSON.parseObject(data, TestBean.class);
                })
                .addSink(JdbcSink.sink(
                        "?",
                        (p, t) -> {
                            p.setString(1,"delete from test where age = 100");
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUrl("jdbc:mysql://localhost:3306/mydb?serverTimezone=Asia/Shanghai")
                                .withUsername("root")
                                .withPassword("123456789")
                                .build()
                ));

        env.execute();
    }
}
