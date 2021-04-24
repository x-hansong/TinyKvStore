package com.xiaohansong.kvstore.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class Position {
    private long start;

    private long len;
}
