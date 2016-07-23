package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    private static final HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();

    private static final LRUCache<String,RecordIndex> orderIndexCache = new LRUCache<>(100000);
    private static final LRUCache<String,RecordIndex> buyerIndexCache = new LRUCache<>(100000);
    private static final LRUCache<String,RecordIndex> goodsIndexCache = new LRUCache<>(100000);


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

    public static OrderSystemImpl.Row queryBuyer(String id) {
        RecordIndex indexCache = buyerIndexCache.get(id);
        if (indexCache == null) {

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
}
