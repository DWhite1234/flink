package utils;

import com.vesoft.nebula.client.graph.data.ResultSet;
import org.apache.flink.api.common.state.MapState;

public class StringUtils {

    public static void setMapState(ResultSet resultSet, MapState<String,String> mapState) {
        try {
            for (String columnName : resultSet.getColumnNames()) {
                String replace = resultSet.colValues(columnName).toString()
                        .replace(">", "")
                        .replace("(","")
                        .replace(")","")
                        .replace("[","")
                        .replace("]","")
                        .replace("\"","");
                String[] split = replace.split("-");
                String key = split[0];
                String value = split[2];
                if (!mapState.contains(key)) {
                    mapState.put(split[0], split[2]);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static StringBuffer getOrganization(MapState<String,String> mapState,String currentCode) {
        StringBuffer buffer = new StringBuffer();
        String initCode = currentCode;
        buffer.append(initCode);
        try {
            Iterable<String> keys = mapState.keys();
            for (String key : keys) {
                String value = mapState.get(initCode);
                buffer.append("-").append(value);
                initCode = value;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }
}
