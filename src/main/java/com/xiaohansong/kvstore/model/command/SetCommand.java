package com.xiaohansong.kvstore.model.command;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SetCommand implements Command{

    private String key;

    private String value;
}
