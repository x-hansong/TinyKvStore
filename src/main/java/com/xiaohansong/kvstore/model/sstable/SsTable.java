package com.xiaohansong.kvstore.model.sstable;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.xiaohansong.kvstore.model.Position;
import com.xiaohansong.kvstore.model.command.Command;
import com.xiaohansong.kvstore.model.command.SetCommand;
import com.xiaohansong.kvstore.utils.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Optional;
import java.util.TreeMap;

public class SsTable {

    private Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    private TableMetaInfo tableMetaInfo;

    private TreeMap<String, Position> sparseIndex;

    private final RandomAccessFile tableFile;

    private final String filePath;


    private SsTable(String filePath, int partSize) {
        this.tableMetaInfo = new TableMetaInfo();
        this.tableMetaInfo.setPartSize(partSize);
        this.filePath = filePath;
        try {
            this.tableFile = new RandomAccessFile(filePath, "rw");
            tableFile.seek(0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        sparseIndex = new TreeMap<>();
    }

    public static SsTable createFromIndex(String filePath, int partSize, TreeMap<String, Command> index) {
        SsTable ssTable = new SsTable(filePath, partSize);
        ssTable.initFromIndex(index);
        return ssTable;
    }

    public static SsTable createFromFile(String filePath) {
        SsTable ssTable = new SsTable(filePath, 0);
        ssTable.restoreFromFile();
        return ssTable;
    }

    public String query(String key) {
        try {
            LinkedList<Position> sparseKeyPositionList = new LinkedList<>();

            Position lastSmallPosition = null;
            Position firstBigPosition = null;

            for (String k : sparseIndex.keySet()) {
                if (k.compareTo(key) <= 0) {
                    lastSmallPosition = sparseIndex.get(k);
                } else {
                    //添加
                    firstBigPosition = sparseIndex.get(k);
                    break;
                }
            }
            if (lastSmallPosition != null) {
                sparseKeyPositionList.add(lastSmallPosition);
            }
            if (firstBigPosition != null) {
                sparseKeyPositionList.add(firstBigPosition);
            }
            if (sparseKeyPositionList.size() == 0) {
                return null;
            }
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseKeyPositionList]: {}", sparseKeyPositionList);
            Position firstKeyPosition = sparseKeyPositionList.getFirst();
            Position lastKeyPosition = sparseKeyPositionList.getLast();
            long start = 0;
            long len = 0;
            start = firstKeyPosition.getStart();
            if (firstKeyPosition.equals(lastKeyPosition)) {
                len = firstKeyPosition.getLen();
            } else {
                len = lastKeyPosition.getStart() + lastKeyPosition.getLen() - start;
            }
            byte[] dataPart = new byte[(int) len];
            tableFile.seek(start);
            tableFile.read(dataPart);
            int pStart = 0;
            for (Position position: sparseKeyPositionList) {
                JSONObject dataPartJson = JSONObject.parseObject(new String(dataPart, pStart, (int)position.getLen()));
                LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][dataPartJson]: {}", dataPartJson);
                if (dataPartJson.containsKey(key)) {
                    return dataPartJson.getString(key);
                }
                pStart += (int) position.getLen();
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    private void restoreFromFile() {
        try {
            TableMetaInfo tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][tableMetaInfo]: {}", tableMetaInfo);
            byte[] indexBytes = new byte[(int) tableMetaInfo.getIndexLen()];
            tableFile.seek(tableMetaInfo.getIndexStart());
            tableFile.read(indexBytes);
            String indexStr = new String(indexBytes, StandardCharsets.UTF_8);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][indexStr]: {}", indexStr);
            sparseIndex = JSONObject.parseObject(indexStr,
                    new TypeReference<TreeMap<String, Position>>() {
                    });
            this.tableMetaInfo = tableMetaInfo;
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseIndex]: {}", sparseIndex);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }


    }

    private void initFromIndex(TreeMap<String, Command> index) {
        try {
            JSONObject partData = new JSONObject(true);
            tableMetaInfo.setDataStart(tableFile.getFilePointer());
            for (Command command : index.values()) {
                //只处理set命令，rm命令可以忽略
                if (command instanceof SetCommand) {
                    SetCommand set = (SetCommand) command;
                    partData.put(set.getKey(), set.getValue());
                }

                //达到分段数量，开始写入数据段
                if (partData.size() >= tableMetaInfo.getPartSize()) {
                    writeDataPart(partData);
                }
            }
            //遍历完之后如果有剩余的数据（尾部数据不一定达到分段条件）写入文件
            if (partData.size() > 0) {
                writeDataPart(partData);
            }
            long dataPartLen = tableFile.getFilePointer() - tableMetaInfo.getDataStart();
            tableMetaInfo.setDataLen(dataPartLen);
            //保存稀疏索引
            byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());
            tableFile.write(indexBytes);
            tableMetaInfo.setIndexLen(indexBytes.length);
            LoggerUtil.debug(LOGGER, "[SsTable][initFromIndex][sparseIndex]: {}", sparseIndex);

            //保存文件索引
            tableMetaInfo.writeToFile(tableFile);
            LoggerUtil.info(LOGGER, "[SsTable][initFromIndex]: {},{}", filePath, tableMetaInfo);

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void writeDataPart(JSONObject partData) throws IOException {
        byte[] partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);
        long start = tableFile.getFilePointer();
        tableFile.write(partDataBytes);

        //记录数据段的第一个key到稀疏索引中
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        firstKey.ifPresent(s -> sparseIndex.put(s, new Position(start, partDataBytes.length)));
        partData.clear();
    }
}
