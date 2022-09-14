package bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;
@Data
public class Pay {
    /**
     * id
     */
    private String id;
    /**
     * 客户号
     */
    private String code;
    /**
     * 客户名称
     */
    private String customName;
    /**
     * 借据号
     */
    private String loanCode;
    /**
     * 币种
     */
    private String currency;
    /**
     * 还款本金
     */
    private BigDecimal payMoney;
    /**
     * 还款利息
     */
    private BigDecimal payInterest;
    /**
     * 还款时间
     */
    private Date payTime;
    /**
     * 贷款余额
     */
    private BigDecimal loanBalance;
    /**
     * 层级关系
     */
    private String keyBy;
    /**
     * 贷款标签
     */
    private String loanLable;
}
