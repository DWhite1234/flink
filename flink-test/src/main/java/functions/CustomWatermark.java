package functions;

import bean.TestBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

@Slf4j
public class CustomWatermark implements WatermarkStrategy<TestBean> {
    @Override
    public WatermarkGenerator<TestBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomWatermarkGenerator();
    }

    private class CustomWatermarkGenerator implements WatermarkGenerator<TestBean>{
        @Override
        public void onEvent(TestBean event, long eventTimestamp, WatermarkOutput output) {
            log.info("onEvent:{}", event);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            log.info("onPeriodicEmit:{}",output);
        }
    }
}
