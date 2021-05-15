package com.xiaohansong.kvstore.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.kvstore.model.command.Command;
import com.xiaohansong.kvstore.model.command.RmCommand;
import com.xiaohansong.kvstore.model.command.SetCommand;
import com.xiaohansong.kvstore.model.sstable.SsTable;
import com.xiaohansong.kvstore.utils.ConvertUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于LsmTree的KV数据库实现
 */
public class LsmKvStore implements KvStore {

    public static final String TABLE = ".table";
    public static final String WAL = "wal";
    public static final String RW_MODE = "rw";
    public static final String WAL_TMP = "walTmp";

    /**
     * 内存表
     */
    private TreeMap<String, Command> index;

    /**
     * 不可变内存表，用于持久化内存表中时暂存数据
     */
    private TreeMap<String, Command> immutableIndex;

    /**
     * ssTable列表
     */
    private final LinkedList<SsTable> ssTables;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁
     */
    private final ReadWriteLock indexLock;

    /**
     * 持久化阈值
     */
    private final int storeThreshold;

    /**
     * 数据分区大小
     */
    private final int partSize;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile wal;

    /**
     * 暂存数据日志文件
     */
    private File walFile;

    /**
     * 初始化
     * @param dataDir 数据目录
     * @param storeThreshold 持久化阈值
     * @param partSize 数据分区大小
     */
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
            //目录为空无需加载ssTable
            if (files == null || files.length == 0) {
                walFile = new File(dataDir + WAL);
                wal = new RandomAccessFile(walFile, RW_MODE);
                return;
            }

            //从大到小加载ssTable
            TreeMap<Long, SsTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (File file : files) {
                String fileName = file.getName();
                //从暂存的WAL中恢复数据，一般是持久化ssTable过程中异常才会留下walTmp
                if (file.isFile() && fileName.equals(WAL_TMP)) {
                    restoreFromWal(new RandomAccessFile(file, RW_MODE));
                }
                //加载ssTable
                if (file.isFile() && fileName.endsWith(TABLE)) {
                    int dotIndex = fileName.indexOf(".");
                    Long time = Long.parseLong(fileName.substring(0, dotIndex));
                    ssTableTreeMap.put(time, SsTable.createFromFile(file.getAbsolutePath()));
                } else if (file.isFile() && fileName.equals(WAL)) {
                    //加载WAL
                    walFile = file;
                    wal = new RandomAccessFile(file, RW_MODE);
                    restoreFromWal(wal);
                }
            }
            ssTables.addAll(ssTableTreeMap.values());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    /**
     * 从暂存日志中恢复数据
     * @param wal
     */
    private void restoreFromWal(RandomAccessFile wal) {
        try {
            long len = wal.length();
            long start = 0;
            wal.seek(start);
            while (start < len) {
                //先读取数据大小
                int valueLen = wal.readInt();
                //根据数据大小读取数据
                byte[] bytes = new byte[valueLen];
                wal.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = ConvertUtil.jsonToCommand(value);
                if (command != null) {
                    index.put(command.getKey(), command);
                }
                start += 4;
                start += valueLen;
            }
            wal.seek(wal.length());
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
            //先保存数据到WAL中
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            index.put(key, command);

            //内存表大小超过阈值进行持久化
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

    /**
     * 切换内存表，新建一个内存表，老的暂存起来
     */
    private void switchIndex() {
        try {
            indexLock.writeLock().lock();
            //切换内存表
            immutableIndex = index;
            index = new TreeMap<>();
            wal.close();
            //切换内存表后也要切换WAL
            File tmpWal = new File(dataDir + WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new RuntimeException("删除文件失败: walTmp");
                }
            }
            if (!walFile.renameTo(tmpWal)) {
                throw new RuntimeException("重命名文件失败: walTmp");
            }
            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile, RW_MODE);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 保存数据到ssTable
     */
    private void storeToSsTable() {
        try {
            //ssTable按照时间命名，这样可以保证名称递增
            SsTable ssTable = SsTable.createFromIndex(dataDir + System.currentTimeMillis() + TABLE, partSize, immutableIndex);
            ssTables.addFirst(ssTable);
            //持久化完成删除暂存的内存表和WAL_TMP
            immutableIndex = null;
            File tmpWal = new File(dataDir + WAL_TMP);
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
            if (command == null) {
                //索引中没有尝试从ssTable中获取，从新的ssTable找到老的
                for (SsTable ssTable : ssTables) {
                    command = ssTable.query(key);
                    if (command != null) {
                        break;
                    }
                }
            }
            if (command instanceof SetCommand) {
                return ((SetCommand) command).getValue();
            }
            if (command instanceof RmCommand) {
                return null;
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
            //删除和写入的操作是一样的
            indexLock.writeLock().lock();
            RmCommand rmCommand = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(rmCommand);
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            index.put(key, rmCommand);
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

    @Override
    public void close() throws IOException {
        wal.close();
        for (SsTable ssTable : ssTables) {
            ssTable.close();
        }
    }
}
