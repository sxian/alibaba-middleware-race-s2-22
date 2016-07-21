package com.alibaba.middleware.race.datastruct;

import java.util.HashSet;
import com.alibaba.middleware.race.OrderSystemImpl.Row;
/**
 * descption: 查找到的索引
 *
 * @author wangwenfeng
 * @date 2016-07-20 15-28
 * @email sxian.wang@gmail.com
 */

public class RecordIndex {
    public String key; // todo 有没有必要
    public String filePath;
    public long position;
    public int length;

    public RecordIndex(String filePath, String key, long position, int length) {
        this.filePath = filePath;
        this.key = key;
        this.position = position;
        this.length = length;
    }

    public RecordIndex(String str) {
        String[] fileds = str.split("\t");
        key = fileds[0];
        filePath = fileds[1];
        position = Long.valueOf(fileds[2]);
        length = Integer.valueOf(fileds[3]);
    }

    @Override
    public String toString() {
        return new StringBuilder().append(key).append('\t').append(filePath).append('\t').append(position)
                .append('\t').append(length).append('\n').toString();
    }
}
