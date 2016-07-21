package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class IndexProcessor {
    //todo asd
    public ArrayList<LinkedBlockingQueue<RecordIndex>> orderIndexQueues;
    public ArrayList<LinkedBlockingQueue<RecordIndex>> buyerIndexQueues;
    public ArrayList<LinkedBlockingQueue<RecordIndex>> goodsIndexQueues;

    public void createOrderIndex(final ArrayList<LinkedBlockingQueue<RecordIndex>> indexQueues) {
        orderIndexQueues = indexQueues;
    }

    public void createBuyerIndex(final ArrayList<LinkedBlockingQueue<RecordIndex>> indexQueues) {
        buyerIndexQueues = indexQueues;
    }

    public void createGoodsIndex(final ArrayList<LinkedBlockingQueue<RecordIndex>> indexQueues) {
        goodsIndexQueues = indexQueues;
    }

}
