package datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.TTLUtil;

import java.util.List;

/**
 * @author zt
 */

public class TestOperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(new MyMap())
                .filter(new MyFilter())
                .print();

        env.execute();
    }


}

class MyMap implements MapFunction<String,String>, CheckpointedFunction{
    public ListState<String> listState;
    public List<String> list;
    @Override
    public String map(String value) throws Exception {
        list.add(value + "map");
        return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        listState.addAll(list);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("buffer", String.class);
        StateTtlConfig hours = TTLUtil.getTTL("hours", 1);
        descriptor.enableTimeToLive(hours);
        listState=context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            Iterable<String> strings = listState.get();
            for (String string : strings) {
                list.add(string);
            }
        }else{

        }

    }
}

class MyFilter implements FilterFunction<String>,CheckpointedFunction{
    public ListState<String> listState;
    @Override
    public boolean filter(String value) throws Exception {
        return true;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
