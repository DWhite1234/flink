package datastream;

import bean.Loan;
import bean.Loan;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TestLoan {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Loan> operator = env.socketTextStream("localhost", 999)
                .map(data -> {
                    Loan loan = JSON.parseObject(data, Loan.class);
                    //1.查询图库,查找对应的层级关系
                    //2.
                    return loan;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Loan>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Loan>() {
                            @Override
                            public long extractTimestamp(Loan element, long recordTimestamp) {
                                return element.getStartTime().getTime();
                            }
                        }));
        operator.keyBy(data -> data.getFirst() + "-" + data.getSecond() + "-" + data.getThird()+"-"+data.getForth()+"-"+data.getFifth())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ProcessWindowFunction<Loan, Loan, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Loan, Loan, String, TimeWindow>.Context context, Iterable<Loan> elements, Collector<Loan> out) throws Exception {

                    }
                });

    }
}
