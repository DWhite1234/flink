package bean;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zt
 */
@Data
public class Cluster {
    private String url;
    private String topic;
    private String message;
}
