package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    // 有没有必要把这里设置为static -> todo 同一个RandomAccessFile 读的时候能共用吗
    private static final HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();

    private static final LRUCache<String,RecordIndex> orderIndexCache = new LRUCache<>(100000);
    private static final LRUCache<String,RecordIndex> buyerIndexCache = new LRUCache<>(100000); // 这两个貌似是1000万条，看下内存占用
    private static final LRUCache<String,RecordIndex> goodsIndexCache = new LRUCache<>(100000);

    public static HashMap<String, TreeMap<Long,Long[]>> filesIndex = new HashMap<>();

    // todo 貌似优化这个地方没卵用
    public static final byte[] _05k = new byte[512];
    public static final byte[] _1k = new byte[1024];
    public static final byte[] _2k = new byte[1024*2];
    public static final byte[] _4k = new byte[1024*4];
    public static final byte[] _8k = new byte[1024*8];

    public static OrderSystemImpl.Row queryOrder(String id) {
        RecordIndex indexCache = orderIndexCache.get(id);
        if (indexCache == null) {

        }
        return queryByIndex(indexCache);
    }

    //
    public static OrderSystemImpl.Row queryBuyer(String id) {
        RecordIndex indexCache = buyerIndexCache.get(id);
        if (indexCache == null) {
            String path = RaceConfig.ORDER_SOTRED_STORE_PATH+(id.hashCode()%RaceConfig.ORDER_FILE_SIZE);
            for (Map.Entry<Long,Long[]> entry : filesIndex.get(path).entrySet()) { // long是啥
                if (entry.getKey().compareTo((Long.valueOf(id))) <= 0) { // todo 确认下b+树的排序方式
                    return queryRowByBPT(path,entry.getKey().toString(),entry.getValue()[0],
                            Integer.valueOf(entry.getValue()[1].toString()));
                }
            }
            return null;
        }
        return queryByIndex(indexCache);
    }

    public static OrderSystemImpl.Row queryGoods(String id) {
        RecordIndex indexCache = goodsIndexCache.get(id);
        if (indexCache == null) {

        }
        return queryByIndex(indexCache);
    }

    private static OrderSystemImpl.Row queryByIndex(RecordIndex index) {
        return queryRow(index.filePath, index.position, index.length);
    }

    private static OrderSystemImpl.Row queryRow(String file, long pos, int length) {
        byte[] bytes = new byte[length];
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                raf = new RandomAccessFile(file,"r");
                randomAccessFileHashMap.put(file, raf);
            }
            raf.seek(pos);
            raf.read(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return OrderSystemImpl.createRow(new String(bytes));
    }

    private static OrderSystemImpl.Row queryRowByBPT(String file, String id, long pos, int length) {
        byte[] bytes = new byte[length];
        boolean findRow = false;
        int rowLen = 0;
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                raf = new RandomAccessFile(file,"r");
                randomAccessFileHashMap.put(file, raf);
            }
            while (!findRow) {
                raf.seek(pos);
                raf.read(bytes);
                String[] bIndexs = new String(bytes,0,length).split(" ");
                if (bIndexs[0].equals("0")) {
                    long _pos = 0;
                    boolean findIndex = false;
                    String[] preIndexPos = bIndexs[1].split(",");
                    if (preIndexPos[0].compareTo(id) == 1) { // 第一个节点满足 todo B+树的节点是是[x,y)吗
                        _pos = Long.valueOf(preIndexPos[1]);
                        length = Integer.valueOf(preIndexPos[2]);
                        findIndex = true;
                    }
                    if (!findIndex) {
                        for (int i = 1; i<bIndexs.length;i++) {
                            String[] indexPos = bIndexs[i+1].split(",");
                            if (i==bIndexs.length-2) {
                                _pos = Long.valueOf(indexPos[1]);
                                length = Integer.valueOf(indexPos[2]);
                                break;
                            }

                            if (preIndexPos[0].compareTo(id) >= 0 && indexPos[0].compareTo(id) == -1) {
                                _pos = Long.valueOf(indexPos[1]);
                                length = Integer.valueOf(indexPos[2]);
                                break;
                            }
                        }
                    }
                    raf.seek(_pos);
                    if (length > bytes.length) {
                        bytes = new byte[length];
                    }
                    raf.read(bytes,0,length);
                } else {
                    for (int i = 1; i<bIndexs.length;i++) {
                        String[] rowPos = bIndexs[i].split(",");
                        if (rowPos[0].equals(id)) {
                            raf.seek(Long.valueOf(rowPos[1]));
                            rowLen = Integer.valueOf(rowPos[2]);
                            if (rowLen > bytes.length) {
                                bytes = new byte[rowLen];
                            }
                            raf.read(bytes,0,rowLen);
                            findRow = true;
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return findRow ? OrderSystemImpl.createRow(new String(bytes,0,rowLen)) : null;
    }
}
