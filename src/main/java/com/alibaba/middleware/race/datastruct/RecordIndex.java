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
    public String pk; // todo 有没有必要
    public String filePath;
    public long position;
    public int length;

    public RecordIndex(String filePath, String pk, long position, int length) {
        this.filePath = filePath;
        this.pk = pk;
        this.position = position;
        this.length = length;
    }
}
