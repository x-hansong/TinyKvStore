package com.xiaohansong.kvstore.model.command;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RmCommand extends AbstractCommand {

    private String key;

    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }
}
