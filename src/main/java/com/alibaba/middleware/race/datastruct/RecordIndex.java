package com.alibaba.middleware.race.datastruct;

import java.util.HashSet;

/**
 * descption: 查找到的索引
 *
 * @author wangwenfeng
 * @date 2016-07-20 15-28
 * @email sxian.wang@gmail.com
 */

public class RecordIndex {
    public String filePath;
    public int position;
    public int length;
    public HashSet<String> keySet;

    public RecordIndex(String filePath, int position) {
        this.filePath = filePath;
        this.position = position;
    }

    public RecordIndex(String filePath, int position, Row row) {

    }
}
