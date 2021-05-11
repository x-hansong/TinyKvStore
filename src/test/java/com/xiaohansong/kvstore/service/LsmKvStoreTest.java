package com.xiaohansong.kvstore.service;

import com.xiaohansong.kvstore.model.sstable.SsTable;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class LsmKvStoreTest {

    @Test
    void set() {
        KvStore kvStore = new LsmKvStore("/Users/hansong/Downloads/kvstore/db/", 4, 3);
        for (int i = 0; i < 11; i++) {
            kvStore.set(i + "", i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertEquals(kvStore.get(i + ""), i + "");
        }
        for (int i = 0; i < 11; i++) {
            kvStore.rm(i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertEquals(kvStore.get(i + ""), null);
        }

    }
}