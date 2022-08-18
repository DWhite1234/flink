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
        } else if ("seconds".equals(unit)) {
            timeUnit = Time.seconds(size);
        }
        return  StateTtlConfig
                .newBuilder(timeUnit)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
    }

    public static StateTtlConfig getTTL(String unit,Integer size,String TTLFlushStrategy) {
        Time timeUnit = null;
        if ("hours".equals(unit)) {
            timeUnit = Time.hours(size);
        }else if("days".equals(unit)){
            timeUnit = Time.days(size);
        } else if ("seconds".equals(unit)) {
            timeUnit = Time.seconds(size);
        }
        StateTtlConfig.UpdateType ttlFlush = StateTtlConfig.UpdateType.OnCreateAndWrite;
        if ("OnReadAndWrite".equals(TTLFlushStrategy)) {
            ttlFlush = StateTtlConfig.UpdateType.OnReadAndWrite;
        }
        return  StateTtlConfig
                .newBuilder(timeUnit)
                .setUpdateType(ttlFlush)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
    }
}
