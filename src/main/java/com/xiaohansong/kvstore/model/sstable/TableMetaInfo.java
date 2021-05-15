package com.xiaohansong.kvstore.model.sstable;

import lombok.Data;

import java.io.RandomAccessFile;

/**
 * ssTable索引信息
 */
@Data
public class TableMetaInfo {

    /**
     * 版本号
     */
    private long version;

    /**
     * 数据区开始
     */
    private long dataStart;

    /**
     * 数据区长度
     */
    private long dataLen;

    /**
     * 索引区开始
     */
    private long indexStart;

    /**
     * 索引区长度
     */
    private long indexLen;

    /**
     * 分段大小
     */
    private long partSize;

    /**
     * 把数据写入到文件中
     * @param file
     */
    public void writeToFile(RandomAccessFile file) {
        try {
            file.writeLong(partSize);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
            file.writeLong(version);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * 从文件中读取元信息，按照写入的顺序倒着读取出来
     * @param file
     * @return
     */
    public static TableMetaInfo readFromFile(RandomAccessFile file) {
        try {
            TableMetaInfo tableMetaInfo = new TableMetaInfo();
            long fileLen = file.length();

            file.seek(fileLen - 8);
            tableMetaInfo.setVersion(file.readLong());

            file.seek(fileLen - 8 * 2);
            tableMetaInfo.setIndexLen(file.readLong());

            file.seek(fileLen - 8 * 3);
            tableMetaInfo.setIndexStart(file.readLong());

            file.seek(fileLen - 8 * 4);
            tableMetaInfo.setDataLen(file.readLong());

            file.seek(fileLen - 8 * 5);
            tableMetaInfo.setDataStart(file.readLong());

            file.seek(fileLen - 8 * 6);
            tableMetaInfo.setPartSize(file.readLong());

            return tableMetaInfo;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }
}
