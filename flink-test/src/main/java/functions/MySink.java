package functions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author zt
 */

public class MySink extends RichSinkFunction<String> {
    public MapState<String, String> mapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, String> flowSum = new MapStateDescriptor<>(
                "sum",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {
                }));
        mapState = getRuntimeContext().getMapState(flowSum);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        mapState.put(value,value);
        System.out.println(mapState.get(value));
    }
}
