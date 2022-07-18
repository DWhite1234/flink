package sum_alert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * 广播
 *
 * @author duhai
 * @date 2020年1月10日
 */
public class BatchDemoBroadcast {

    public static void main(final String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<Tuple2<String, Integer>>();
        broadData.add(new Tuple2<String, Integer>("zs", 18));
        broadData.add(new Tuple2<String, Integer>("ls", 20));
        broadData.add(new Tuple2<String, Integer>("ww", 17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        // 1.1:处理需要广播的数据,把数据集转换成map类型，map中的key就是用户姓名，value就是用户年龄
        DataSet<HashMap<String, Integer>> toBroadcast = tupleData
                .map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                    public HashMap<String, Integer> map(final Tuple2<String, Integer> value) throws Exception {
                        HashMap<String, Integer> res = new HashMap<String, Integer>();
                        res.put(value.f0, value.f1);
                        return res;
                    }
                });

        // 源数据
        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        // 注意：在这里需要使用到RichMapFunction获取广播变量
        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            /**
             * 这个方法只会执行一次 可以在这里实现一些初始化的功能。所以，就可以在open方法中获取广播变量数据
             *
             */
            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                // 3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }

            }

            @Override
            public String map(final String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");
        result.print();
    }
}


