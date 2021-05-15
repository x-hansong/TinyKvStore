package com.xiaohansong.kvstore.service;


import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class LsmKvStoreTest {

    @Test
    public void set() throws IOException {
        KvStore kvStore = new LsmKvStore("/Users/hansong/Downloads/kvstore/db/", 4, 3);
        for (int i = 0; i < 11; i++) {
            kvStore.set(i + "", i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertEquals(i + "", kvStore.get(i + ""));
        }
        for (int i = 0; i < 11; i++) {
            kvStore.rm(i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertNull(kvStore.get(i + ""));
        }
        kvStore.close();
        kvStore = new LsmKvStore("/Users/hansong/Downloads/kvstore/db/", 4, 3);
        for (int i = 0; i < 11; i++) {
            assertNull(kvStore.get(i + ""));
        }
        kvStore.close();
    }

}