package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.IndexCache;
import com.alibaba.middleware.race.datastruct.Index;
import com.alibaba.middleware.race.datastruct.RecordIndex;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    private static HashMap<String, RandomAccessFile> indexFileMap = new HashMap<>();
    private static HashMap<String, RandomAccessFile> dataFileMap = new HashMap<>();

    public static final ConcurrentHashMap<String,int[][]> indexMap = new ConcurrentHashMap<>();
//    public static ConcurrentHashMap<String, TreeMap<String,int[]>> filesIndex = new ConcurrentHashMap<>();
//    public static ConcurrentHashMap<String, ArrayList<String>> filesIndexKey = new ConcurrentHashMap<>();

    public static IndexCache buyerIndex = new IndexCache(2000000);
    public static IndexCache goodIndex = new IndexCache(1000000);

    public static void initFile() {
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

        String index = getIndex(id, path,true);
        if (index != null) {
            String[] indexs = index.split(","); // id path pos len
            if (indexs[0].equals(id)) {
                RandomAccessFile raf = dataFileMap.get(indexs[1]);
                return queryData(raf,Long.valueOf(indexs[2]),Integer.valueOf(indexs[3].trim()));
            }
        }
        return null;
    }

    public static String queryBuyer(String id) throws IOException {
        String path = RaceConfig.DISK1+"b/iS"+Math.abs(id.hashCode()%RaceConfig.BUYER_FILE_SIZE);
        int[] indexs = getBGIndex(id, path, buyerIndex);
        RandomAccessFile raf =  dataFileMap.get(RaceConfig.DISK1+"b/"+Math.abs(id.hashCode()%RaceConfig.BUYER_FILE_SIZE));
        return  queryData(raf,indexs[0],indexs[1]);
    }

    public static String queryGoods(String id) throws IOException {
        String path = RaceConfig.DISK2+"g/iS"+Math.abs(id.hashCode()%RaceConfig.GOODS_FILE_SIZE);
        int[] index = getBGIndex(id, path, goodIndex);
        if(index==null) {
            return null;
        }
        RandomAccessFile raf = dataFileMap.get(RaceConfig.DISK2+"g/"+Math.abs(id.hashCode()%RaceConfig.GOODS_FILE_SIZE));
        return  queryData(raf,index[0],index[1]);
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
        String index = getIndex(buyerid, path,true);
        ArrayList<String> querys = new ArrayList<>();
        if (index != null) {
            int split = index.indexOf(":");
            String id = index.substring(0,split);
            String[] orders = index.substring(split+1).split(";");
            if (buyerid.equals(id)) {
                for (String orderid : orders) {
                    int _split = orderid.lastIndexOf(",");
                    long time = Long.valueOf(orderid.substring(_split+1));
                    if (time>= start && time<end) {
                        querys.add(orderid);
                    }
                }
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
        String index = getIndex(goodid, path, true);
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

    public static int[] getBGIndex(String id, String path, IndexCache cache) throws IOException {
        int[] index = cache.get(id);
        if (index==null){
            String _indexs = getIndex(id,path,false);
            if (_indexs!=null) {
                cache.put(_indexs);
                int in = _indexs.indexOf(id);
                String indexs = _indexs.substring(in,_indexs.indexOf(" ",in));
                int first = indexs.indexOf(",");
                int last = indexs.lastIndexOf(",");
                index = new int[]{Integer.valueOf(indexs.substring(first+1,last)),Integer.valueOf(indexs.substring(last+1))};
            }
        }
        return index;
    }

    public static String getIndex(String id, String path, boolean flag) throws IOException {// true order false buyer goods
        int bucket = Math.abs(id.hashCode()%Index.BUCKET_SIZE);
        int[] pos = indexMap.get(path)[bucket];
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
        return flag ? str.substring(in,str.indexOf(" ",in)) : str;
    }

    public static String queryData(RandomAccessFile raf, long pos, int length) throws IOException {
        byte[] bytes = new byte[length];
        synchronized (raf) {
            raf.seek(pos);
            raf.read(bytes);
        }
        return new String(bytes);
    }

    public static String queryData(String path, long pos, int length) throws IOException {
        RandomAccessFile raf = dataFileMap.get(path);
        return queryData(raf,pos,length);
    }

    public static List<String> batchQuery(List<String> ids, String goodid) throws IOException {
        List<String> result = new ArrayList<>(); // path pos length orderid
        Collections.sort(ids, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                long p1 = Long.valueOf(o1.substring(o1.indexOf(",")+1,o1.lastIndexOf(",")));
                long p2 = Long.valueOf(o2.substring(o2.indexOf(",")+1,o2.lastIndexOf(",")));

                return p1 > p2 ? 1 : (p1 < p2 ? -1 : 0);
            }
        });
        String first = ids.get(0);
        String last = ids.get(ids.size()-1);
        RandomAccessFile raf = dataFileMap.get(first.substring(0,first.indexOf(",")));
        int start_pos = Integer.valueOf(first.substring(first.indexOf(",")+1,first.lastIndexOf(",")));
        int end_pos = Integer.valueOf(last.substring(last.indexOf(",")+1,last.lastIndexOf(",")))+
                Integer.valueOf(last.substring(last.lastIndexOf(",")+1));

        synchronized (raf) {
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY,start_pos,end_pos-start_pos);
            mbb.isLoaded();
            byte[] bytes = new byte[1024];
            for (String index: ids) {
                int start = index.indexOf(",")+1;
                int end = index.lastIndexOf(",");
                int pos = Integer.valueOf(index.substring(start,end))-start_pos;
                int len = Integer.valueOf(index.substring(end+1));
                if (bytes.length < len) {
                    bytes = new byte[len];
                }
                for (int i = 0;i<len;i++) {
                    bytes[i] = mbb.get(pos+i);
                }
                result.add(new String(bytes, 0, len));
            }
        }
        return result;
    }
}
