package bean;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class Banlance {

    public Banlance(String account_Balance_Id, String accounting_Item_Code, String interbank_Account, String internal_Account, BigDecimal balance, String financial_Date, Date gmt_Modify, Date gmt_Correct) {
        this.account_Balance_Id = account_Balance_Id;
        this.accounting_Item_Code = accounting_Item_Code;
        this.interbank_Account = interbank_Account;
        this.internal_Account = internal_Account;
        this.balance = balance;
        this.financial_Date = financial_Date;
    }

    public String account_Balance_Id;

    public String accounting_Item_Code;

    public String interbank_Account;
    public String internal_Account;
    public BigDecimal balance;
    public String financial_Date;

    public Timestamp gmt_Modify;
    public Timestamp gmt_Correct;

}
