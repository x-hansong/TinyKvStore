package com.xiaohansong.kvstore.model.command;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class AbstractCommand implements Command {

    private CommandTypeEnum type;

    public AbstractCommand(CommandTypeEnum type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
