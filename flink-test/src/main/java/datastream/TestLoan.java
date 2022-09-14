package datastream;

import bean.Loan;
import bean.Pay;
import com.alibaba.fastjson.JSON;

import com.vesoft.nebula.client.graph.data.ResultSet;
import functions.NebulaSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.NebulaUtil;
import utils.StringUtils;

public class TestLoan {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Loan> loanDetail = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Loan.class))
//                .addSink(new NebulaSink());
                .keyBy(data -> data.getCode())
                .map(new RichMapFunction<Loan, Loan>() {
                    public MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, String.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public Loan map(Loan value) throws Exception {
                        ResultSet resultTag = NebulaUtil.get(
                                "use flink_test;" +
                                        "match (v1:customer)-[e0:t1]->(v2:manager)-[e1:t2]->(v3:orgnize)\n" +
                                        "where id(v1)==\"001\" and id(v3) ==\"003\"\n" +
                                        "return e0,e1");
                        StringUtils.setMapState(resultTag, mapState);
                        StringBuffer keyBy = StringUtils.getOrganization(mapState, "001");
                        value.setKeyBy(keyBy.toString());
                        return value;
                    }
                });

        env.socketTextStream("localhost", 888)
                .map(data -> JSON.parseObject(data, Pay.class))
                .keyBy(data -> data.getCode())
                .map(new RichMapFunction<Pay, Pay>() {
                    public MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, String.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public Pay map(Pay value) throws Exception {
                        ResultSet resultTag = NebulaUtil.get(
                                "use flink_test;" +
                                        "match (v1:customer)-[e0:t1]->(v2:manager)-[e1:t2]->(v3:orgnize)\n" +
                                        "where id(v1)==\"001\" and id(v3) ==\"003\"\n" +
                                        "return e0,e1");
                        StringUtils.setMapState(resultTag, mapState);
                        StringBuffer keyBy = StringUtils.getOrganization(mapState, "001");
                        value.setKeyBy(keyBy.toString());
                        return value;
                    }
                });

        env.execute();
    }
}
