package com.xiaohansong.kvstore.model;

import com.xiaohansong.kvstore.model.command.Command;
import com.xiaohansong.kvstore.model.command.RmCommand;
import com.xiaohansong.kvstore.model.command.SetCommand;
import com.xiaohansong.kvstore.model.sstable.SsTable;
import org.junit.jupiter.api.Test;

import java.util.TreeMap;

class SsTableTest {

    @Test
    void createFromIndex() {
        TreeMap<String, Command> index = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            SetCommand setCommand = new SetCommand("key" + i, "value" + i);
            index.put(setCommand.getKey(), setCommand);
        }
        index.put("key100", new RmCommand("key100"));
        SsTable ssTable = SsTable.createFromIndex("test.txt", 3, index);
    }

    @Test
    void createFromFile() {
        SsTable ssTable = SsTable.createFromFile("test.txt");
    }
}