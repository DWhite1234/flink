package pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author zt
 */
@Data
public class Envelope<T>{
    private Head head;
    private T body;

    @Data
    public static class Head {
        /** 应用无须赋值，由SDK自动生成 */
        private String sdkVersion;

        /** UUID 应用无须赋值，由SDK自动生成 */
        private String msgSeqId;

        /** 链路痕迹ID,由SDK产生,如果在ncp收到消息没有traceId,要求把msgSeqId赋给traceId */
        private String traceId;

        /** 主题名称, 每一条消息都应该指定投递的主题 */
        private String topic;

        /** 消息有效期, 消息有效截止时间 默认为空, 长期有效 */
        private long gmtExpire;

        /** 当前应用的唯一表示, 在平台创建应用后自动分配 */
        private String producerAppId;

        /** 生产者系统IP */
        private String ip;

        /** 生产者系统MAC */
        private String mac;

        /** 指定想要让那些应用来消费, 多个值逗号隔开, 默认为空 都可以消费此消息 */
        private String allowedConsumerAppIds;

        /** 指定不要让哪些应用消费，多个值逗号隔开，默认为空, 都可以消费 */
        private String refusedConsumeAppIds;

        /** 交易类型,  用来声明当前消息归属的业务类型 */
        private String transactionType;

        /** 交易码原子级操作 */
        private String transactionCode;

        /** 协同流程中消息专用，记录状态机流程实例ID */
        private String cpInstId;

        /** 协同流程中消息专用，记录状态机流程定义ID */
        private String cpDefId;

        /** 消息创建时间戳, sdk 接收消息时生成 */
        private long gmtCreate;

        /** 消息发送时间戳, sdk提交消息时生成 */
        private long gmtSubmit;

        /** 邮差接收到主题的时间戳 */
        private long gmtReceive;
        /** 消息流转时间戳, 由邮差提交消息时生成或更新 */
        private long gmtDeliver;

        /** 邮差scp接收到主题的时间戳 */
        private long gmtScpReceive;
        /** 消息流转时间戳, 由邮差scp提交消息时生成或更新 */
        private long gmtScpDeliver;

        /** 消息接收时间戳, 由sdk监听到消息反序列化时生成 */
        private long gmtConsume;

        /** 消息存活期 */
        private int ttl;

        /* ncp-flink 添加的属性 */
        /** 投递流程 */
        private List<DeliveryProcess> deliveryProcesses;
        /** 过滤规则 */
        private String filterRule;
        /** 转换规则 */
        private String transferRule;
        /** 目标主题 */
        private String targetTopic;
        /** 当前主题详情 */
        private Topic currentTopicDetail;
        /** 目标主题详情 */
        private Topic targetTopicDetail;
        /** 所有源主题详情信息 */
        private List<TopicGroup> sourceTopicsDetails;
        /** 所有目标主题详情信息*/
        private Map<String, Topic> targetTopicsDetails;
        /** 去重字段 */
        private String distinctColumn;
        /** 事务id */
        private String txId;
        /** 协同流 */
        private String context;
    }
}
