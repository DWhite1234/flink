package utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

public class JDBCtuilsTest {

    @Test
    public void select() {
        ArrayList<String> list = JDBCutils.select("select * from product");
        System.out.println(list);
    }
}
