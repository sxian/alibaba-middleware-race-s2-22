package com.alibaba.middleware.race.datastruct;

import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sxian.wang on 2016/7/30.
 */
public class AssistIndex extends Index {
    public AssistIndex(String path) {
        super();
        FILE_PATH = path;
        for (int i = 0;i<BUCKET_SIZE;i++) {
            _list[i] = new HashMap<>();
        }
    }

    private HashMap<String,ArrayList>[] _list = new HashMap[BUCKET_SIZE];

    public AssistIndex(boolean flag) {
        super(flag);
    }

    @Override
    public void add(String key, String index) {
        ArrayList<String> list = _list[Math.abs(key.hashCode()%BUCKET_SIZE)].get(key);
        if (list == null) {
            list = new ArrayList<>();
            _list[Math.abs(key.hashCode()%BUCKET_SIZE)].put(key,list);
        }
        list.add(index.substring(index.indexOf(",")+1,index.length()));
    }

    @Override
    public int[][] writeToDisk() throws IOException {
        BufferedWriter bw = Utils.createWriter(FILE_PATH);
        int[][] result = new int[BUCKET_SIZE][2];
        int pos = 0;
        StringBuilder _sb = new StringBuilder();
        for (int i = 0;i<BUCKET_SIZE;i++) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String,ArrayList> entry : _list[i].entrySet()) {
                ArrayList<String> list = entry.getValue();
                sb.append(entry.getKey()).append(":");
                for (int j = 0;j<list.size();j++) {
                    sb.append(list.get(j)).append(".");
                }
                sb.append(" ");
            }
            int length = sb.toString().getBytes().length;
            _sb.append(sb.toString());
            if (i%3==0) {
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
