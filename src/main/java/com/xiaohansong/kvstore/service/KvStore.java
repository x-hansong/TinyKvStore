package com.xiaohansong.kvstore.service;

import com.xiaohansong.kvstore.model.Result;

public interface KvStore {

    void set(String key, String value);

    String get(String key);

    void rm(String key);

}
