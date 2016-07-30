package com.alibaba.middleware.race.datastruct;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Created by sxian.wang on 2016/7/29.
 */
public class Index {
    public static final int BUCKET_SIZE = 2000;
    public String FILE_PATH;
    public boolean flag;

    private ArrayList<String>[] _list = new ArrayList[BUCKET_SIZE];

    public Index(){};

    public Index(String path) {
        FILE_PATH = path;
        for (int i = 0;i<BUCKET_SIZE;i++) {
            _list[i] = new ArrayList<>();
        }
    }

    public Index(boolean flag) {
        FILE_PATH = "";
        this.flag = flag;
    }

    public void add(String key,String index) {
        _list[Math.abs(key.hashCode()%BUCKET_SIZE)].add(index);
    }

    public int[][] writeToDisk() throws IOException {
        BufferedWriter bw = Utils.createWriter(FILE_PATH);
        int[][] result = new int[BUCKET_SIZE][2];
        int pos = 0;
        StringBuilder _sb = new StringBuilder();
        for (int i = 0;i<BUCKET_SIZE;i++) {
            ArrayList<String> list = _list[i];
            _list[i] = null;
//            Collections.sort(list);
            StringBuilder sb = new StringBuilder();
            for (int j = 0;j<list.size();j++) {
                sb.append(list.get(j)).append(" ");
            }
            int length = sb.toString().getBytes().length;
            _sb.append(sb.toString());
            if (i%5==0) {
                bw.write(_sb.toString());
                _sb.delete(0,_sb.length());
            }
            result[i][0] = pos;
            result[i][1] = length;
            pos += length;
        }
        bw.write(_sb.toString());
        bw.flush();
        bw.close();
        return result;
    }
}
