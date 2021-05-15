package com.xiaohansong.kvstore.model.command;

/**
 * 命令接口
 */
public interface Command {

    /**
     * 获取数据key
     * @return
     */
    String getKey();
}
