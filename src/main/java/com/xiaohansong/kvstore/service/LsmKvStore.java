package com.xiaohansong.kvstore.service;

import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.kvstore.model.command.Command;
import com.xiaohansong.kvstore.model.command.RmCommand;
import com.xiaohansong.kvstore.model.command.SetCommand;
import com.xiaohansong.kvstore.model.sstable.SsTable;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmKvStore implements KvStore {

    public static final String TABLE = ".table";
    private TreeMap<String, Command> index;

    private TreeMap<String, Command> immutableIndex;

    private LinkedList<SsTable> ssTables;

    private String dataDir;

    private ReadWriteLock indexLock;

    /**
     * 持久化阈值
     */
    private int storeThreshold;

    private int partSize;

    private RandomAccessFile wal;

    private File walFile;

    private RandomAccessFile immutableWal;

    public LsmKvStore(String dataDir, int storeThreshold, int partSize) {
        try {
            this.dataDir = dataDir;
            this.storeThreshold = storeThreshold;
            this.partSize = partSize;
            this.indexLock = new ReentrantReadWriteLock();
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            ssTables = new LinkedList<>();
            index = new TreeMap<>();
            if (files == null|| files.length == 0) {
                walFile = new File(dataDir + "wal");
                wal = new RandomAccessFile(walFile, "rw");
                return;
            }

            TreeMap<Long, SsTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i].getName();
                if (files[i].isFile() && fileName.endsWith(TABLE)) {
                    int dotIndex = fileName.indexOf(".");
                    Long time = Long.parseLong(fileName.substring(0, dotIndex));
                    ssTableTreeMap.put(time, SsTable.createFromFile(files[i].getAbsolutePath()));
                } else if (files[i].isFile() && fileName.equals("wal")) {
                    walFile = files[i];
                    wal = new RandomAccessFile(files[i], "rw");
                }
            }
            ssTables.addAll(ssTableTreeMap.values());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }


    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            indexLock.writeLock().lock();
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            index.put(key, command);

            if (index.size() > storeThreshold) {
                switchIndex();
                storeToSsTable();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }

    }

    private void switchIndex() {
        try {
            indexLock.writeLock().lock();
            immutableIndex = index;
            index = new TreeMap<>();
            wal.close();
            File tmpWal = new File(dataDir + "walTmp");
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new RuntimeException("删除文件失败: walTmp");
                }
            }
            if (!walFile.renameTo(tmpWal)) {
                throw new RuntimeException("重命名文件失败: walTmp");
            }
            walFile = new File(dataDir + "wal");
            wal = new RandomAccessFile(walFile, "rw");
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private void storeToSsTable() {
        try {
            SsTable ssTable = SsTable.createFromIndex(dataDir + System.currentTimeMillis() + TABLE, partSize, immutableIndex);
            ssTables.addFirst(ssTable);
            immutableIndex = null;
            File tmpWal = new File(dataDir + "walTmp");
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new RuntimeException("删除文件失败: walTmp");
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }


    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            //先从索引中取
            Command command = index.get(key);
            //再尝试从不可变索引中取，此时可能处于持久化sstable的过程中
            if (command == null && immutableIndex != null) {
                command = immutableIndex.get(key);
            }
            if (command instanceof SetCommand) {
                return ((SetCommand) command).getValue();
            }
            if (command instanceof RmCommand) {
                return null;
            }
            //索引中没有尝试从ssTable中获取，从新的ssTable找到老的
            for (SsTable ssTable : ssTables) {
                String value = ssTable.query(key);
                if (value != null) {
                    return value;
                }
            }
            //找不到说明不存在
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }

    }

    @Override
    public void rm(String key) {
        try {
            indexLock.writeLock().lock();
            RmCommand rmCommand = new RmCommand(key);
            index.put(key, rmCommand);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }
}
