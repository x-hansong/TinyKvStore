package com.xiaohansong.kvstore.model.command;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RmCommand implements Command {

    private String key;
}
