package functions;

import bean.Loan;
import com.alibaba.fastjson.JSON;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import utils.NebulaUtil;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class NebulaSink extends RichSinkFunction<Loan> {

    @Override
    public void invoke(Loan value, Context context) throws Exception {
        String code = value.getCode();
        /*
        1.明细节点应该会把,点的具体的类型给到,直接插入即可
        2.余额节点同样,只需要取借据号做vid,由还款明细的借据号进行覆盖即可
        3.标签节点是同样的道理,每个贷款标签,插入对应的tag,用同一个借据号即可
         */
//        ResultSet list = NebulaUtil.get(
//                "use flink_test;"+
//                "insert vertex pay(code,money) values \"" + 999 + "\":(\"" + "小微贷款" + "\"," + 666 + ");");

        ResultSet resultSet = NebulaUtil.get(
                "use flink_test;"+
                "MATCH (v0:`customer`)-[e0:`t1`]->(v1:`manager`)-[e1:`t2`]->(v2:`orgnize`)\n" +
                "WHERE  ( id(v0)  == \"001\")  AND  ( id(v2)  == \"003\") \n" +
                "RETURN e0,e1 LIMIT 100;");
        for (String columnName : resultSet.getColumnNames()) {
            List<ValueWrapper> valueWrappers = resultSet.colValues(columnName);
//            System.out.println();
        }
        ResultSet resultTag = NebulaUtil.get(
                "match (v1:customer)-[e0:t1]->(v2:manager)-[e1:t2]->(v3:orgnize)\n" +
                        "where id(v1)==\"001\" and id(v3) ==\"003\"\n" +
                        "return e0,e1");

        HashMap<String, String> map = new HashMap<>();
        for (String columnName : resultTag.getColumnNames()) {
            String replace = resultTag.colValues(columnName).toString()
                    .replace(">", "")
                    .replace("(","")
                    .replace(")","")
                    .replace("[","")
                    .replace("]","")
                    .replace("\"","");
            String[] split = replace.split("-");
            map.put(split[0], split[2]);
        }
        System.out.println(map);
        StringBuffer buffer = new StringBuffer();
        String init = "001";
        do {
            String res = map.get(init);
            buffer.append(res);
            init = res;
        } while (!init.equals("003"));
        System.out.println(buffer);
    }
}
