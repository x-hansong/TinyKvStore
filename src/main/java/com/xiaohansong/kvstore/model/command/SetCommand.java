package com.xiaohansong.kvstore.model.command;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SetCommand extends AbstractCommand {

    private String key;

    private String value;

    public SetCommand(String key, String value) {
        super(CommandTypeEnum.SET);
        this.key = key;
        this.value = value;
    }
}
