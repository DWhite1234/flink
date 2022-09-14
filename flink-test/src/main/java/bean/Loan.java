package bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;
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
    private String currency;
    /**
     * 贷款期限
     */
    private Integer loanPeriod;
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
     * 贷款品种
     */
    private String LoanType;
    /**
     * 是否科技贷款
     */
    private boolean isScienceLoan;
    /**
     * 是否新基建
     */
    private boolean isInfrastructureLoan;
    /**
     * 担保方式
     */
    private String guaranteeType;
    /**
     * 四级分类
     */
    private String forthClassification;
    /**
     * 五级分类
     */
    private String fifthClassification;
    /**
     * 抵质押物类型
     */
    private String pledgeType;
    /**
     * 抵质押物名称
     */
    private String pledgeName;
    /**
     * 抵质押物所有人
     */
    private String pledgeOwner;
    /**
     * 保证人
     */
    private String guarantor;
    /**
     * 支付方式
     */
    private String payType;
    /**
     * 层级关系
     */
    private String keyBy;
    /**
     * 贷款标签
     */
    private String loanLable;

}
