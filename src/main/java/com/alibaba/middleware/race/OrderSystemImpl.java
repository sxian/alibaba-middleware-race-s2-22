package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImpl implements OrderSystem {
    @Override
    public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles, Collection<String> storeFolders) throws IOException, InterruptedException {

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        return null;
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        return null;
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        return null;
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        return null;
    }
}
