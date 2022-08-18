package bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@Data
public class Loan {
    private String id;
    /**
     * 客户号
     */
    private String code;
    /**
     * 客户名称
     */
    private String shortName;
    /**
     * 借据号
     */
    private String loanCode;
    /**
     * 币种
     */
    private String currentType;
    /**
     * 放款金额
     */
    private BigDecimal loanMoney;
    /**
     * 发放时间
     */
    private Date startTime;
    /**
     * 到期日
     */
    private Date endTime;
    /**
     * 一级机构-总行
     */
    private String first;
    /**
     * 二级机构-分行/直属支行
     */
    private String second;
    /**
     * 三级机构-支行
     */
    private String third;
    /**
     * 四级机构-团队
     */
    private String forth;
    /**
     * 五级机构-客户经理
     */
    private String fifth;
    /**
     * 贷款标签 用string/list
     */
    private List<String> list;

}
