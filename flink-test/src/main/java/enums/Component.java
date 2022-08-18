package enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Component {
    EBOX("EBOX","ebox"),
    EBOX_CONSUMER("EBOX_CONSUMER","eboxConsumer")
    ;
    private String code;
    private String name;
}
