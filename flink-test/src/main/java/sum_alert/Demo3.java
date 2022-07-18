package sum_alert;

import bean.Banlance;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class Demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        Name	Code	Data Type	Length	Precision	Mandatory	Primary Identifier	Displayed	Domain
        账户余额Id	account_Balance_Id	Variable characters (32)	32		TRUE	TRUE	TRUE	字符串主键
        会计科目号	accounting_Item_Code	Variable characters (32)	32		FALSE	FALSE	TRUE	短名称
        同业账户	interbank_Account	Variable characters (64)	64		FALSE	FALSE	TRUE	名称
        内部账户	internal_Account	Variable characters (64)	64		FALSE	FALSE	TRUE	名称
        余额	balance	Decimal (19,2)	19	2	FALSE	FALSE	TRUE	金额
        财务日期	financial_Date	Long variable characters (17)	17		FALSE	FALSE	TRUE	日期时间字符
        修改时间	gmt_Modify	Date & Time			FALSE	FALSE	TRUE	时间
        矫正时间	gmt_Correct	Date & Time			FALSE	FALSE	TRUE	时间
         */

        env.fromElements(new Banlance("","3","3","3",new BigDecimal(100),"2022-05-06 12:23:22",new Timestamp(new Date().getTime()),new Date()))
            .addSink(JdbcSink.sink(
                    "insert into Position_Account_Balance (account_Balance_Id, accounting_Item_Code, interbank_Account, internal_Account,balance,financial_Date,gmt_Modify,gmt_Correct) values (?,?,?,?,?,?,?,?)",
                    (ps, t) -> {
                        ps.setString(1, UUID.randomUUID().toString().replace(",",""));
                        ps.setString(2, t.accounting_Item_Code);
                        ps.setString(3, t.interbank_Account);
                        ps.setString(4,t.internal_Account);
                        ps.setBigDecimal(5,t.balance);
                        ps.setString(6,t.financial_Date);
                        ps.setTimestamp(7,t.gmt_Modify);
                        ps.setTimestamp(8,t.gmt_Correct);
                    },
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:mysql://localhost:3306/mydb")
                            .withDriverName("com.mysql.cj.jdbc.Driver")
                            .withUsername("root")
                            .withPassword("123456789")
                            .build()
            ));

        env.execute();

    }
}
