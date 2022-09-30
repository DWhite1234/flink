package pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author zt
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicGroup {
    /** 队列集群编号 */
    private String queueClusterCode;

    /** 队列集群地址 */
    private String queueClusterAddr;

    /** 投递分组编号 */
    private String code;

    /** 消费线程配置 */
//    private PostmanConfig config;

    /** 是否有序 */
    private Boolean ordered;

    /** 主题编号列表 */
    private List<Topic> topics;
}
