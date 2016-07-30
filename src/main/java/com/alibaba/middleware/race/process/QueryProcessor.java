package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.Index;
import com.alibaba.middleware.race.datastruct.RecordIndex;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    // 有没有必要把这里设置为static -> todo 同一个RandomAccessFile 读的时候能共用吗 -> 并发下使用不安全
    private static HashMap<String, RandomAccessFile> indexFileMap = new HashMap<>();
    private static HashMap<String, RandomAccessFile> dataFileMap = new HashMap<>();

    public static final ConcurrentHashMap<String,int[][]> indexMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, TreeMap<String,int[]>> filesIndex = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, ArrayList<String>> filesIndexKey = new ConcurrentHashMap<>();

    public static void initFile() {
//        loadCache();
        try {
            for (int i = 0;i<RaceConfig.ORDER_FILE_SIZE;i++) {
                RandomAccessFile raf1 = new RandomAccessFile(RaceConfig.DISK1+"o/"+i,"r");
                RandomAccessFile raf2 = new RandomAccessFile(RaceConfig.DISK2+"o/"+i,"r");
                RandomAccessFile raf3 = new RandomAccessFile(RaceConfig.DISK3+"o/"+i,"r");

                RandomAccessFile raf4 = new RandomAccessFile(RaceConfig.DISK1+"o/iS"+i,"r");
                RandomAccessFile raf5 = new RandomAccessFile(RaceConfig.DISK2+"o/iS"+i,"r");
                RandomAccessFile raf6 = new RandomAccessFile(RaceConfig.DISK3+"o/iS"+i,"r");

                dataFileMap.put(RaceConfig.DISK1+"o/"+i,raf1);
                dataFileMap.put(RaceConfig.DISK2+"o/"+i,raf2);
                dataFileMap.put(RaceConfig.DISK3+"o/"+i,raf3);

                indexFileMap.put(RaceConfig.DISK1+"o/iS"+i,raf4);
                indexFileMap.put(RaceConfig.DISK2+"o/iS"+i,raf5);
                indexFileMap.put(RaceConfig.DISK3+"o/iS"+i,raf6);
            }
            for (int i = 0;i<RaceConfig.BUYER_FILE_SIZE;i++) {
                RandomAccessFile raf1 = new RandomAccessFile(RaceConfig.DISK1+"b/"+i,"r");
                dataFileMap.put(RaceConfig.DISK1+"b/"+i,raf1);

                RandomAccessFile raf2 = new RandomAccessFile(RaceConfig.DISK1+"b/iS"+i,"r");
                indexFileMap.put(RaceConfig.DISK1+"b/iS"+i,raf2);
            }
            for (int i = 0;i<RaceConfig.GOODS_FILE_SIZE;i++) {
                RandomAccessFile raf1 = new RandomAccessFile(RaceConfig.DISK2+"g/"+i,"r");
                dataFileMap.put(RaceConfig.DISK2+"g/"+i,raf1);

                RandomAccessFile raf2 = new RandomAccessFile(RaceConfig.DISK2+"g/iS"+i,"r");
                indexFileMap.put(RaceConfig.DISK2+"g/iS"+i,raf2);
            }

            for (int i = 0;i<RaceConfig.HB_FILE_SIZE;i++) {
                RandomAccessFile raf1 = new RandomAccessFile(RaceConfig.DISK1+"o/hgS"+i,"r");
                RandomAccessFile raf2 = new RandomAccessFile(RaceConfig.DISK2+"o/hgS"+i,"r");
                RandomAccessFile raf3 = new RandomAccessFile(RaceConfig.DISK3+"o/hgS"+i,"r");

                RandomAccessFile raf4 = new RandomAccessFile(RaceConfig.DISK1+"o/hbS"+i,"r");
                RandomAccessFile raf5 = new RandomAccessFile(RaceConfig.DISK2+"o/hbS"+i,"r");
                RandomAccessFile raf6 = new RandomAccessFile(RaceConfig.DISK3+"o/hbS"+i,"r");

                indexFileMap.put(RaceConfig.DISK1+"o/hgS"+i,raf1);
                indexFileMap.put(RaceConfig.DISK2+"o/hgS"+i,raf2);
                indexFileMap.put(RaceConfig.DISK3+"o/hgS"+i,raf3);

                indexFileMap.put(RaceConfig.DISK1+"o/hbS"+i,raf4);
                indexFileMap.put(RaceConfig.DISK2+"o/hbS"+i,raf5);
                indexFileMap.put(RaceConfig.DISK3+"o/hbS"+i,raf6);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void loadCache() {
        if (indexMap.size() == 323) {
            return;
        }
        BufferedReader br = null;
        System.out.println("*** start load cache ***");
        try {
            br = Utils.createReader(RaceConfig.DISK1+"indexCache");
            String line = br.readLine();
            int[][] index = null;
            String path = "";
            int i = 0;
            boolean init = false;
            int count = 0;
            while (line!=null) {
                if (line.charAt(0) == 'F' && line.charAt(1) == 'i' && line.charAt(2) == 'l' && line.charAt(3) == 'e') {
                    System.out.println("start load: "+ line);
                    count++;
                    if (init) {
                        indexMap.put(path, index);
                    } else {
                        init = true;
                    }
                    index = new int[Index.BUCKET_SIZE][2];
                    path = line.substring(line.indexOf(":")+1);
                    i = 0;
                    line = br.readLine();
                    continue;
                }
                int split = line.indexOf(",");
                index[i][0] = Integer.valueOf(line.substring(0,split));
                index[i][1] = Integer.valueOf(line.substring(split+1));
                i++;
                line = br.readLine();
            }
            System.out.println("load cache complete, file num: " + count);
            indexMap.put(path,index);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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

        String index = getIndex(id, path);
        if (index != null) {
            String[] indexs = index.split(",");
            if (indexs[0].equals(id)) {
                RandomAccessFile raf = dataFileMap.get(indexs[1]);
                return queryData(raf,Long.valueOf(indexs[2]),Integer.valueOf(indexs[3].trim())); // todo 记得去掉空格
            }
        }
        return null;
    }

    public static String queryBuyer(String id) throws IOException {
        String path = RaceConfig.DISK1+"b/iS"+Math.abs(id.hashCode()%RaceConfig.BUYER_FILE_SIZE);
        String[] indexs = getIndex(id, path).split(",");
        if (indexs[0].equals(id)) {
            RandomAccessFile raf =  dataFileMap.get(RaceConfig.DISK1+"b/"+Math.abs(id.hashCode()%RaceConfig.BUYER_FILE_SIZE));
            return  queryData(raf,Long.valueOf(indexs[1]),Integer.valueOf(indexs[2].trim()));
        }
        return null;
    }

    public static String queryGoods(String id) throws IOException {
        String path = RaceConfig.DISK2+"g/iS"+Math.abs(id.hashCode()%RaceConfig.GOODS_FILE_SIZE);
        String[] indexs = getIndex(id, path).split(","); // todo 修改
        if (indexs[0].equals(id)) {
            RandomAccessFile raf = dataFileMap.get(RaceConfig.DISK2+"g/"+Math.abs(id.hashCode()%RaceConfig.GOODS_FILE_SIZE));
            return  queryData(raf,Long.valueOf(indexs[1]),Integer.valueOf(indexs[2].trim()));
        }
        return null;
    }

    public static List<String> queryOrderidsByBuyerid(String buyerid, long start, long end) throws IOException {
        int disk = Math.abs(buyerid.hashCode()%3);
        int file = Math.abs(buyerid.hashCode()%RaceConfig.HB_FILE_SIZE);
        String path = null;
        switch (disk) {
            case 0:
                path = RaceConfig.DISK1+"o/hbS"+file;
                break;
            case 1:
                path = RaceConfig.DISK2+"o/hbS"+file;
                break;
            case 2:
                path = RaceConfig.DISK3+"o/hbS"+file;
                break;
        }
        String index = getIndex(buyerid, path);
        ArrayList<String> querys = new ArrayList<>();
        if (index != null) {
            int split = index.indexOf(":");
            String id = index.substring(0,split);
            String[] orders = index.substring(split+1).split(";");
            if (buyerid.equals(id)) {
                for (String orderid : orders) {
                    int _split = orderid.indexOf(",");
                    long time = Long.valueOf(orderid.substring(0,_split));
                    if (time>= start && time<end) {
                        querys.add(orderid);
                    }
                }
                Collections.sort(querys, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        int split1 = o1.indexOf(",");
                        int split2 = o2.indexOf(",");
                        long time1 = Long.valueOf(o1.substring(0,split1));
                        long time2 = Long.valueOf(o2.substring(0,split2));

                        return time1 > time2 ? -1 : (time1 < time2  ?  1 : 0);
                    }
                });
//                ArrayList<String> result = new ArrayList<>(); todo 改成一趟查完
//
//                for (String orderid : querys) {
//                    result.add(queryOrder(orderid.substring(orderid.indexOf(",")+1)));
//                }
            }
        }
        return  querys;
    }

    public static List<String> queryOrderidsByGoodsid(String goodid) throws IOException {
        int disk = Math.abs(goodid.hashCode()%3);
        int file = Math.abs(goodid.hashCode()%RaceConfig.HG_FILE_SIZE);
        String path = null;
        switch (disk) {
            case 0:
                path = RaceConfig.DISK1+"o/hgS"+file;
                break;
            case 1:
                path = RaceConfig.DISK2+"o/hgS"+file;
                break;
            case 2:
                path = RaceConfig.DISK3+"o/hgS"+file;
                break;
        }
        String index = getIndex(goodid, path);
        if (index != null) {
            int split = index.indexOf(":");
            String id = index.substring(0,split);
            if (goodid.equals(id)) {
                String[] orders = index.substring(split+1).split(";");
                return Arrays.asList(orders);
            }
        }
        return  new ArrayList<>();
    }

    public static String getIndex(String id, String path) throws IOException {
        int bucket = Math.abs(id.hashCode()%Index.BUCKET_SIZE);
        if (indexMap.get(path) == null) {
            int i =1;
        }
        int[]   pos = indexMap.get(path)[bucket];
        if (pos[1]==0) {
            return null;
        }

        byte[] bytes = new byte[pos[1]];
        RandomAccessFile raf = indexFileMap.get(path);
        synchronized (raf) {
            raf.seek(pos[0]);
            raf.read(bytes);
        }
        String str = new String(bytes);
        int in = str.indexOf(id);
        if (in == -1) {
            return null;
        }
        return str.substring(in,str.indexOf(" ",in));
    }

    public static String queryIndex(String id, String path) throws IOException {
        ArrayList<String> idKeys = filesIndexKey.get(path);
        String key;
        try {

        if (id.compareTo(idKeys.get(0))< 0) {
            key = idKeys.get(0);
        } else if(id.compareTo(idKeys.get(idKeys.size()-1)) > 0) {
            key = idKeys.get(idKeys.size()-1);
        } else {
            key = binarySearchString(idKeys,id);
        }
        int[] pos = null;
            pos = filesIndex.get(path).get(key);
        return queryRowStringByBPT(path, id, pos[0],Integer.valueOf(String.valueOf(pos[1])));
        } catch (Exception e) {
            int i = 1;
        }
        return null;
    }

    public static String queryData(RandomAccessFile raf, long pos, int length) throws IOException {
        byte[] bytes = new byte[length];
        synchronized (raf) {
            raf.seek(pos);
            raf.read(bytes);
        }
        return new String(bytes);
    }


    public static List<String> queryRangeOrder(TreeMap<String,Long[]> map, ArrayList<String> list, String path,
                                               String id, Long start, Long end) throws IOException {
        Long[] pos = map.get(id);
        if (pos == null) {
            return new ArrayList<>();
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
        return null;
    }

    private static String queryRow(String file, long pos, int length) {
        byte[] bytes = new byte[length];
        try {
            RandomAccessFile raf = indexFileMap.get(file);
            synchronized (raf) {
                raf.seek(pos);
                raf.read(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String(bytes);
    }

    private static String queryRowStringByBPT(String file, String id, long pos, int length) throws IOException {
        byte[] bytes = new byte[length];
        boolean findRow = false;
        int rowLen = 0;

        RandomAccessFile raf = indexFileMap.get(file);
            while (!findRow) {
                synchronized (raf) {
                    raf.seek(pos);
                    raf.read(bytes);
                }
                String rawStr = new String(bytes,0,bytes.length);
                // todo
                String[] bIndexs = rawStr.split(" ");
                Arrays.sort(bIndexs);
                if (bIndexs.length == 1) {
                    return rawStr;
                }
                if (bIndexs[1].equals("0")) {
                    boolean findIndex = false;
                    String[] preIndexPos = bIndexs[2].split(",");
                    if (preIndexPos[0].compareTo(id)>0) { // 第一个节点满足
                        pos = Long.valueOf(preIndexPos[1]);
                        length = Integer.valueOf(preIndexPos[2]);
                        continue;
                    }
                    String[] lastIndex = bIndexs[bIndexs.length-1].split(",");
                    if (lastIndex[0].compareTo(id)<=0) {
                        pos = Long.valueOf(lastIndex[1]);
                        length = Integer.valueOf(lastIndex[2]);
                        continue;
                    }
                    if (!findIndex) {
                        for (int i = 2; i<bIndexs.length;i++) {
                            String[] indexPos = bIndexs[i+1].split(",");
                            if (preIndexPos[0].compareTo(id) <= 0 && indexPos[0].compareTo(id) > 0 ) {
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }
                            preIndexPos = indexPos;
                        }
                    }
                } else if (bIndexs[1].equals("1")){
                    for (int i = 2; i<bIndexs.length;i++) {
                        String[] rowPos = bIndexs[i].split(",");
                        if (rowPos[0].equals(id)) {
                            synchronized (raf) {
                                raf.seek(Long.valueOf(rowPos[1]));
                                rowLen = Integer.valueOf(rowPos[2]);
                                if (rowLen > bytes.length) {
                                    bytes = new byte[rowLen];
                                }
                                raf.read(bytes,0,rowLen);
                            }
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
