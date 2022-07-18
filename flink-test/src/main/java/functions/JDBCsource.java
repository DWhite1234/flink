package functions;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.JDBCutils;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/*
countDownLatch定时查询mysql

countDownLatch:
    作用:阻塞线程

 */
public class JDBCsource implements SourceFunction<String> {
    //定义n个阻塞线程
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            ArrayList<String> select = JDBCutils.select("select * from product");
            sourceContext.collect(select.toString());
            //设置阻塞线程的阻塞时间
            countDownLatch.await(1, TimeUnit.SECONDS);
        }
    }

    @Override
    public void cancel() {

    }
}
