package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    // 有没有必要把这里设置为static -> todo 同一个RandomAccessFile 读的时候能共用吗
    private static final HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();

    private static final LRUCache<String,RecordIndex> orderIndexCache = new LRUCache<>(100000); // orderid 5e 这个地方索引缓存
    private static final LRUCache<String,RecordIndex> buyerIndexCache = new LRUCache<>(100000); // buyerid 800w 看下内存占用
    private static final LRUCache<String,RecordIndex> goodsIndexCache = new LRUCache<>(100000); // goodid 400w

    public static HashMap<String, TreeMap<Long,Long[]>> filesIndex = new HashMap<>();
    public static HashMap<String, Long[]> filesIndexKey = new HashMap<>();

    public static void addIndexCache(RecordIndex index, int flag) {
        if (flag == 0) {
            buyerIndexCache.put(index.key,index);
        } else {
            goodsIndexCache.put(index.key,index);
        }
    }

    public static String queryOrder(String id) {
        RecordIndex indexCache = orderIndexCache.get(id);  // todo orderid的索引缓存是有问题的，和rawdata重叠了
        if (indexCache == null) {
            String path = RaceConfig.ORDER_SOTRED_STORE_PATH+"oS"+(id.hashCode()%RaceConfig.ORDER_FILE_SIZE);
            long orderid = Long.valueOf(id);

            Long[] idKeys = filesIndexKey.get(path);
            if (idKeys==null) {
                idKeys = (Long[]) filesIndex.get(path).keySet().toArray();
                filesIndexKey.put(path, idKeys);
            }

            long key;
            if (orderid<idKeys[0]) {
                key = idKeys[0];
            } else if(orderid>idKeys[idKeys.length-1]) {
                key = idKeys[idKeys.length-1];
            } else {
                key = binarySearch(idKeys,orderid);
            }
            Long[] pos = filesIndex.get(path).get(key);
            return queryRowByBPT(path, orderid, pos[0],Integer.valueOf(String.valueOf(pos[1])));
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
    public static String queryBuyer(String id) {
        RecordIndex indexCache = buyerIndexCache.get(id);
        if (indexCache == null) {
            throw new RuntimeException("buyerid index error");
        }
        return queryByIndex(indexCache);
    }

    public static String queryGoods(String id) {
        RecordIndex indexCache = goodsIndexCache.get(id);
        if (indexCache == null) {
            throw new RuntimeException("goodsid index error");
        }
        return queryByIndex(indexCache);
    }

    private static String queryByIndex(RecordIndex index) {
        return queryRow(index.filePath, index.position, index.length);
    }

    private static String queryRow(String file, long pos, int length) {
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
        return new String(bytes);
    }

    private static String queryRowByBPT(String file, long orderid, long pos, int length) {

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
                    if (Long.valueOf(preIndexPos[0])>orderid) { // 第一个节点满足
                        pos = Long.valueOf(preIndexPos[1]);
                        length = Integer.valueOf(preIndexPos[2]);
                        continue;
                    }
                    if (!findIndex) {
                        for (int i = 1; i<bIndexs.length-1;i++) {
                            String[] indexPos = bIndexs[i+1].split(",");
                            if (i==bIndexs.length-2) {
                                if (orderid < Long.valueOf(preIndexPos[0])) {
                                    throw new RuntimeException("compare error");
                                }
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }

                            if (Long.valueOf(preIndexPos[0]) <= orderid && Long.valueOf(indexPos[0]) > orderid) {
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
                        try {
                            if (Long.valueOf(rowPos[0])==orderid) {
                                raf.seek(Long.valueOf(rowPos[1]));
                                rowLen = Integer.valueOf(rowPos[2]);
                                if (rowLen > bytes.length) {
                                    bytes = new byte[rowLen];
                                }
                                raf.read(bytes,0,rowLen);
                                findRow = true;
                                break;
                            }
                        } catch (Exception e) {
                            int a =0;
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

        return findRow ? new String(bytes,0,rowLen) : null;
    }

    public static long binarySearch(Long[] data, long key) {
        int min, max, mid;
        min = 0;
        max = data.length - 1;
        while(!((max-min)== 1)){
            mid = (min + max) / 2;
            if(key < data[mid]){
                max = mid;  // 因为不清楚data[mid-1]是不是满足 key<data[mid-1],可能存在key>=data[min+1]的情况
            }else if (key > data[mid]){
                min = mid;  // 因为不清楚data[mid+1]是不是满足 key>=data[mid+1],可能存在data[min+1]>key的情况
            }else if(key == data[mid]){
                return data[mid];
            }
        }

        if (data[max] <= key) {
            return data[max];
        }
        return data[min];
    }
}
