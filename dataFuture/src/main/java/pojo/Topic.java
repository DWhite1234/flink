package pojo;

import com.myouchai.infoex.core.processor.qo.ClusterInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zt
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Topic {
    /** 主题编号 */
    private String code;

    /** 关键字 */
    private String keyBy;

    /** 级别 */
    private String level;

    /** 分区数 */
    private Integer partitions;

    /** jsonSchema */
    private String content;

    /** 是否去重 */
    private Boolean isDistinct;

    /** 去重关键字 */
    private String distinctKeyword;

    /** 去重关键字EL表达式 */
    private String distinctKeywordElExp;

    /** 是否有序 */
    private Boolean ordered;

    /** 队列集群编号 */
    private String queueClusterCode;

    /** 队列集群地址 */
    private String queueClusterAddr;

    /**
     * 外部集群鉴权信息
     */
    private ClusterInfo clusterInfo;
}
