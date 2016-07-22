package com.alibaba.middleware.race.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    public static HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();

    public static String query(String file, long pos, int length) throws IOException {
        RandomAccessFile raf = randomAccessFileHashMap.get(file);
        if (raf == null) {
            raf = new RandomAccessFile(file,"r");
            randomAccessFileHashMap.put(file, raf);
        }

        byte[] bytes = new byte[length];
        raf.seek(pos);
        raf.read(bytes);
        return new String(bytes);
    }
}
