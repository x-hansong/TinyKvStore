package com.xiaohansong.kvstore.model.command;

import lombok.Getter;
import lombok.Setter;

/**
 * 删除命令
 */
@Getter
@Setter
public class RmCommand extends AbstractCommand {

    /**
     * 数据key
     */
    private String key;

    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }
}
