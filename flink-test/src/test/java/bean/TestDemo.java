package bean;

import net.sf.cglib.beans.BeanCopier;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class TestDemo {

    @Test
    public void demo0() {
        DecimalFormat decimalFormat = new DecimalFormat("###,###.00");
        System.out.println(decimalFormat.format(new BigDecimal("12341")));
    }

    @Test
    public void demo2() {
        Person person = new Person();
        person.setAge(0);

        cal(person);

        System.out.println(person);
    }

    public void cal(Person person) {
        if (person.getName()==null) {
            Person person1 = new Person();
            person1.setName("11");
            BeanCopier beanCopier = BeanCopier.create(Person.class, Person.class, false);
            beanCopier.copy(person1,person,null);
            System.out.println(person);
        }else {
            person.setAge(person.getAge() + 1);
        }
    }
}
