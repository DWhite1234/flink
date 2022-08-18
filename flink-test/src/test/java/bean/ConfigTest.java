package bean;

import enums.Component;
import org.apache.flink.util.StringUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ConfigTest {

    @Test
    public void Demo01() {
        Config config = new Config();
        config.setAge(18);
        System.out.println(config.getAge());
        System.out.println(config.toString());
    }

    @Test
    public void Demo2() {
        String topic = String.format("([a-zA-Z]+\\.%s)|^%s$", "topic","topic");
        System.out.println(topic);
        Pattern pattern = Pattern.compile(topic);
        Matcher matcher = pattern.matcher("aa.topic");
        System.out.println(matcher.find()+"-"+matcher.group());
    }

    @Test
    public void Demo3() {
        String code = Component.EBOX.getCode();
    }

    @Test
    public void Demo4() {
        BigDecimal decimal = new BigDecimal("1000.11");
        String format = NumberFormat.getInstance().format(decimal);
        System.out.println(format);
    }
    @Test
    public void Demo5() {
        BigDecimal bigDecimal = new BigDecimal("19999");
        BigDecimal divide = bigDecimal.divide(new BigDecimal("10000"),2, RoundingMode.DOWN);
        System.out.println(divide);
    }
}