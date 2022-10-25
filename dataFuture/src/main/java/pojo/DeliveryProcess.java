package pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

/**
 * @author zt
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeliveryProcess {

    private String deliveryProcessId;

    private String code;

    private String name;

    private String status;

    private List<String> sourceTopicCodes;

    private List<String> defTopicCodes;

    private List<String> insTopicCodes;

    private List<String> targetTopicCodes;

    //private List<TransferRule> transferRuleVos;

    /** 过滤规则 */
    //private List<FilterRule> filterRuleVos;

    private String postman;

    private String content;

    private String creator;

    private Date gmtCreate;

    private String modifier;

    private Date gmtModify;
}
