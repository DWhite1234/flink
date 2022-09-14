package utils;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class NebulaUtil {
    public static NebulaPool nebulaPool;
    public static Session session;
    public static ResultSet result = null;

    static {
        try {
            nebulaPool = new NebulaPool();
            NebulaPoolConfig config = new NebulaPoolConfig();
            config.setMaxConnSize(1);
            List<HostAddress> address = Arrays.asList(new HostAddress("172.1.2.152",9669));
            nebulaPool.init(address,config);
            session = nebulaPool.getSession("root","123",false);
        } catch (Exception e) {
            log.info("nebula 连接池异常.....,{}",e.getMessage());
        }finally {
            nebulaPool.close();
        }
    }

    public static ResultSet get(String nGql) {
        try {
            result = session.execute(nGql);
        } catch (Exception e) {
            log.info("session execute异常.....,{}",e.getMessage());
        }
        return result;
    }

    public static void get(String nGql, MapState<String,String> mapSate) {
        try {
            result = session.execute(nGql);

        } catch (Exception e) {
            log.info("session execute异常.....,{}",e.getMessage());
        }
    }
}
