package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    // 有没有必要把这里设置为static -> todo 同一个RandomAccessFile 读的时候能共用吗 -> 并发下使用不安全
    private static final HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();
    private static final HashMap<String, RandomAccessFile> dataFileMap = new HashMap<>();

    public static HashMap<String, TreeMap<String,int[]>> filesIndex = new HashMap<>();
    public static HashMap<String, ArrayList<String>> filesIndexKey = new HashMap<>();

    public void initFile() {
        // todo
    }

    public static String queryOrder(String id) throws IOException {
        int disk = Math.abs(id.hashCode()%3);
        int file = Math.abs(id.hashCode()%RaceConfig.ORDER_FILE_SIZE);
        String path;
        if (disk == 0) {
            path = RaceConfig.DISK1+"o/iS"+file;
        } else if(disk == 1) {
            path = RaceConfig.DISK2+"o/iS"+file;

        } else {
            path = RaceConfig.DISK3+"o/iS"+file;
        }
        String[] indexs = queryIndex(id,path).split(",");
        if (indexs[0].equals(id)) {
            RandomAccessFile raf = new RandomAccessFile(indexs[1],"r");
            return queryData(raf,Long.valueOf(indexs[2]),Integer.valueOf(indexs[3].trim())); // todo 记得去掉空格
        }
        return null;
    }

    public static String queryBuyer(String id) throws IOException {
        String path = RaceConfig.DISK1+"b/iS"+Math.abs(id.hashCode()%RaceConfig.BUYER_FILE_SIZE);
        String[] indexs = queryIndex(id, path).split(",");
        if (indexs[0].equals(id)) {
            RandomAccessFile raf = new RandomAccessFile(RaceConfig.DISK1+"b/"+Math.abs(id.hashCode()%RaceConfig.BUYER_FILE_SIZE),"r");
            return  queryData(raf,Long.valueOf(indexs[1]),Integer.valueOf(indexs[2].trim()));
        }
        return null;
    }

    public static String queryGoods(String id) throws IOException {
        String path = RaceConfig.DISK2+"g/iS"+Math.abs(id.hashCode()%RaceConfig.GOODS_FILE_SIZE);
        String[] indexs = queryIndex(id, path).split(",");
        if (indexs[0].equals(id)) {
            RandomAccessFile raf = new RandomAccessFile(RaceConfig.DISK2+"g/"+Math.abs(id.hashCode()%RaceConfig.GOODS_FILE_SIZE),"r");
            return  queryData(raf,Long.valueOf(indexs[1]),Integer.valueOf(indexs[2].trim()));
        }
        return null;
    }

    public static String queryIndex(String id, String path) throws IOException {
        ArrayList<String> idKeys = filesIndexKey.get(path);
        String key;
        if (id.compareTo(idKeys.get(0))< 0) {
            key = idKeys.get(0);
        } else if(id.compareTo(idKeys.get(idKeys.size()-1)) > 0) {
            key = idKeys.get(idKeys.size()-1);
        } else {
            key = binarySearchString(idKeys,id);
        }
        int[] pos = filesIndex.get(path).get(key);
        return queryRowStringByBPT(path, id, pos[0],Integer.valueOf(String.valueOf(pos[1])));
    }

    public static String queryData(RandomAccessFile raf, long pos, int length) throws IOException {
        byte[] bytes = new byte[length];
        synchronized (raf) {
            raf.seek(pos);
            raf.read(bytes);
        }
        return new String(bytes);
    }

    public static List<String> queryOrderidsByBuyerid(String buyerid, long start, long end) {
        return  null;
    }

    public static List<String> queryOrderidsByGoodsid(String goodid) {
        return  null;
    }

    public static List<String> queryRangeOrder(TreeMap<String,Long[]> map, ArrayList<String> list, String path,
                                               String id, Long start, Long end){
//        String key;
//        if (id.compareTo(list.get(0)) < 0) {
//            key = list.get(0);
//        } else if(id.compareTo(list.get(list.size()-1)) > 0) {
//            key = list.get(list.size()-1);
//        } else {
//            key = binarySearchString(list, id);
//        }
        Long[] pos = map.get(id);
        if (pos == null) {
            return new ArrayList<>(); // todo 索引全加载到内存可以这样做，如果分开的话return null的操作不应该在这里
//            throw new RuntimeException("range query order id error");
        }

        String result = queryRowStringByBPT(path, id, pos[0],Integer.valueOf(String.valueOf(pos[1])));
        if (result == null) {
            return null;
        }
        List<String> orderList = Arrays.asList(result.split(" "));
        List<String> newList = new ArrayList<>();
        if (start != null) {
            for (String str : orderList) {  // orderList 是无序的
                String[] kv = str.split(",");
                long time = 0;
                try {
                    time = Long.valueOf(kv[1]);
                } catch (Exception e) {
                    int i = 1;
                }
                if (time>= start && time < end){
                    // todo 在处理文件数据的时候换下位置
                    newList.add(kv[1]+","+kv[0]); // 因为要按时间排序，所以返回有序的结果 -> 不能光添加订单，否则会按照订单排序
                }
            }
            Collections.sort(newList, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return -(o1.compareTo(o2));
                }
            });
            return newList;
        }
        Collections.sort(newList);  // 光有订单id, 按照订单id升序排列(从小到大)
        return orderList;
    }

    private static String queryByIndex(RecordIndex index) {
        return queryRow(index.filePath, index.position, index.length);
    }

    private static String queryRow(String file, long pos, int length) {
        byte[] bytes = new byte[length];
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                synchronized (QueryProcessor.class) {
                    raf = randomAccessFileHashMap.get(file);
                    if (raf == null) {
                        raf = new RandomAccessFile(file,"r");
                        randomAccessFileHashMap.put(file, raf);
                    }
                }
            }
            synchronized (raf) {
                raf.seek(pos);
                raf.read(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String(bytes);
    }

    // todo 可以用queryRowStringByBPT替代queryRowByBPT
    private static String queryRowByBPT(String file, long orderid, long pos, int length) {
        byte[] bytes = new byte[length];
        boolean findRow = false;
        int rowLen = 0;
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                synchronized (QueryProcessor.class) {
                    raf = randomAccessFileHashMap.get(file);
                    if (raf == null) {
                        raf = new RandomAccessFile(file,"r");
                        randomAccessFileHashMap.put(file, raf);
                    }
                }
            }
            synchronized (raf) {

                while (!findRow) {
                    raf.seek(pos);
                    raf.read(bytes);
                    String rawStr = new String(bytes,0,length);
                    String[] bIndexs = rawStr.split(" ");
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
                    } else if (bIndexs[0].equals("1")) {
                        for (int i = 1; i<bIndexs.length;i++) {
                            if (bIndexs[i].equals("\n")) {  // todo 最终版把写到文件的\n全删了
                                continue;
                            }
                            String[] rowPos = bIndexs[i].split(",");
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
                        }
                        break;
                    } else {
                        if (Long.valueOf(rawStr.split("\t")[0].split(":")[1])==orderid) {
                            return rawStr;
                        }
                        return null;
                    }
                    if (length > bytes.length) {
                        bytes = new byte[length];
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return findRow ? new String(bytes,0,rowLen) : null;
    }

    private static String queryRowStringByBPT(String file, String id, long pos, int length) {
        byte[] bytes = new byte[length];
        boolean findRow = false;
        int rowLen = 0;
        try {

            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                synchronized (QueryProcessor.class) {
                    raf = randomAccessFileHashMap.get(file);
                    if (raf == null) {
                        raf = new RandomAccessFile(file,"r");
                        randomAccessFileHashMap.put(file, raf);
                    }
                }
            }
            synchronized (raf) {
                while (!findRow) {
                    raf.seek(pos);
                    raf.read(bytes);
                    String rawStr = new String(bytes,0,length);
                    String[] bIndexs = rawStr.split(" ");
                    if (bIndexs[0].equals("0")) {
                        boolean findIndex = false;
                        String[] preIndexPos = bIndexs[1].split(",");
                        if (preIndexPos[0].compareTo(id)>0) { // 第一个节点满足
                            pos = Long.valueOf(preIndexPos[1]);
                            length = Integer.valueOf(preIndexPos[2]);
                            continue;
                        }
                        if (!findIndex) {
                            for (int i = 1; i<bIndexs.length-1;i++) {
                                String[] indexPos = bIndexs[i+1].split(",");
                                if (i==bIndexs.length-2) {
                                    if (id.compareTo(preIndexPos[0]) < 0) {
                                        throw new RuntimeException("compare error");
                                    }
                                    pos = Long.valueOf(preIndexPos[1]);
                                    length = Integer.valueOf(preIndexPos[2]);
                                    break;
                                }

                                if (preIndexPos[0].compareTo(id) <= 0 && indexPos[0].compareTo(id) > 0 ) {
                                    pos = Long.valueOf(preIndexPos[1]);
                                    length = Integer.valueOf(preIndexPos[2]);
                                    break;
                                }
                                preIndexPos = indexPos;
                            }
                        }
                    } else if (bIndexs[0].equals("1")){
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
                                break;
                            }
                        }
                        break;
                    } else {
                        return rawStr;
                    }
                    if (length > bytes.length) {
                        bytes = new byte[length];
                    }
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

    private static String binarySearchString(ArrayList<String> data, String key) {
        int min, max, mid;
        min = 0;
        max = data.size() - 1;
        while(!((max-min)== 1)){
            mid = (min + max) / 2;
            if(key.compareTo(data.get(mid))<0){
                max = mid;  // 因为不清楚data[mid-1]是不是满足 key<data[mid-1],可能存在key>=data[min+1]的情况
            }else if (key.compareTo(data.get(mid))>0){
                min = mid;  // 因为不清楚data[mid+1]是不是满足 key>=data[mid+1],可能存在data[min+1]>key的情况
            }else if(key.equals(data.get(mid))){
                return data.get(mid);
            }
        }

        if (data.get(max).compareTo(key) <=0 ) {
            return data.get(max);
        }
        return data.get(min);
    }


}
