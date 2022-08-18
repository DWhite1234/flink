package utils;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

public class TTLUtil {
    public static StateTtlConfig getTTL(String unit,Integer size) {
        Time timeUnit = null;
        if ("hours".equals(unit)) {
            timeUnit = Time.hours(size);
        }else if("days".equals(unit)){
            timeUnit = Time.days(size);
        }
        return  StateTtlConfig
                .newBuilder(timeUnit)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }
}
