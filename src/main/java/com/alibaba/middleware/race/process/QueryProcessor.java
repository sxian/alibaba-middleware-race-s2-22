package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
            String path = RaceConfig.ORDER_SOTRED_STORE_PATH+"oS"+(id.hashCode()%RaceConfig.ORDER_FILE_SIZE);
            Map.Entry<Long,Long[]> preEntry = null;
            long orderid = Long.valueOf(id);
            for (Map.Entry<Long,Long[]> entry : filesIndex.get(path).entrySet()) { // long是啥 todo 线性查找太慢，改算法
                if (preEntry== null) {
                    if (entry.getKey().compareTo(orderid) == 1) {
                        return queryRowByBPT(path,id,entry.getValue()[0],
                                Integer.valueOf(entry.getValue()[1].toString()));
                    }
                } else {
                    if (preEntry.getKey().compareTo(orderid) <=0 && entry.getKey().compareTo(orderid) == 1) {
                        return queryRowByBPT(path, id,preEntry.getValue()[0],
                                Integer.valueOf(preEntry.getValue()[1].toString()));
                    }
                }
                preEntry = entry;
            }
            return queryRowByBPT(path, id, preEntry.getValue()[0],Integer.valueOf(preEntry.getValue()[1].toString()));
        }
        return queryByIndex(indexCache);
    }

    public static List<String> queryOrderidsByBuyerid(String buyerid, long start, long end) {
        // todo
        return null;
    }

    public static List<String> queryOrderidsByGoodsid(String goodid) {
        // todo
        return null;
    }

    // buyer和goods的索引全在内存里面。
    public static OrderSystemImpl.Row queryBuyer(String id) {
        RecordIndex indexCache = buyerIndexCache.get(id);
        if (indexCache == null) {
            throw new RuntimeException("buyerid index error");
        }
        return queryByIndex(indexCache);
    }

    public static OrderSystemImpl.Row queryGoods(String id) {
        RecordIndex indexCache = goodsIndexCache.get(id);
        if (indexCache == null) {
            throw new RuntimeException("goodsid index error");
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

    private static OrderSystemImpl.Row queryRowByBPT(String file, String orderid, long pos, int length) {
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
                    boolean findIndex = false;
                    String[] preIndexPos = bIndexs[1].split(",");
                    if (preIndexPos[0].compareTo(orderid) > 0) { // 第一个节点满足
                        pos = Long.valueOf(preIndexPos[1]);
                        length = Integer.valueOf(preIndexPos[2]);
                        continue;
                    }
                    if (!findIndex) {
                        for (int i = 1; i<bIndexs.length-1;i++) {
                            String[] indexPos = bIndexs[i+1].split(",");
                            if (i==bIndexs.length-3) {
                                if (orderid.compareTo(indexPos[0]) < 0) {
                                    throw new RuntimeException("compare error");
                                }
                                pos = Long.valueOf(indexPos[1]);
                                length = Integer.valueOf(indexPos[2]);
                                break;
                            }

                            if (preIndexPos[0].compareTo(orderid) <= 0 && indexPos[0].compareTo(orderid) > 0) {
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }
                            preIndexPos = indexPos;
                        }
                    }
                } else {
                    for (int i = 1; i<bIndexs.length;i++) {
                        String[] rowPos = bIndexs[i].split(",");
                        if (rowPos[0].equals(orderid)) {
                            raf.seek(Long.valueOf(rowPos[1]));
                            rowLen = Integer.valueOf(rowPos[2]);
                            if (rowLen > bytes.length) {
                                bytes = new byte[rowLen];
                            }
                            raf.read(bytes,0,rowLen);
                            findRow = true;
                            break;
                        }
                    }
                    break;
                }
                if (length > bytes.length) {
                    bytes = new byte[length];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return findRow ? OrderSystemImpl.createRow(new String(bytes,0,rowLen)) : null;
    }

}
