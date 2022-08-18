package bean;

import lombok.Data;

import java.util.Date;

@Data
public class Person {
    private String name;
    private Integer age;
    private Date ts;
}
