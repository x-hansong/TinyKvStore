package com.xiaohansong.kvstore.model;

import lombok.Data;

@Data
public class Result<T> {

    private T data;

    private boolean success;

    private String msg;
}
