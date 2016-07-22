package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    public static HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();

    // todo 貌似优化这个地方没卵用
    public static final byte[] _05k = new byte[512];
    public static final byte[] _1k = new byte[1024];
    public static final byte[] _2k = new byte[1024*2];
    public static final byte[] _4k = new byte[1024*4];
    public static final byte[] _8k = new byte[1024*8];
    HashMap<String,String> orderCache = new HashMap<>();

    BplusTree<RecordIndex> orderTree;
    BplusTree<RecordIndex> buyerTree;
    BplusTree<RecordIndex> goodsTree;

    public QueryProcessor() {
        try {
            orderTree = IndexProcessor.buildIndexTree(Arrays.asList(new String[]{
                    "t/index/orderIndex_0","t/index/orderIndex_1","t/index/orderIndex_2"}));
            buyerTree = IndexProcessor.buildIndexTree(Arrays.asList(new String[]{"t/index/buyerIndex_0"}));
            goodsTree = IndexProcessor.buildIndexTree(Arrays.asList(new String[]{"t/index/goodsIndex_0"}));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String query(String file, long pos, int length) throws IOException {
        RandomAccessFile raf = randomAccessFileHashMap.get(file);
        byte[] bytes = new byte[length];
        if (raf == null) {
            raf = new RandomAccessFile(file,"r");
            randomAccessFileHashMap.put(file, raf);
        }
        raf.seek(pos);
        raf.read(bytes);
        return new String(bytes);
    }

    public static String queryByIndex(RecordIndex index) throws IOException {
        return query(index.filePath, index.position, index.length);
    }

    public String queryOrder(String orderId) throws IOException {
        String result = orderCache.get(orderId);
        if (result == null) {
            RecordIndex index = orderTree.get(orderId);
            result = queryByIndex(index);
            orderCache.put(orderId,result);
        }
        return result;
    }
}
