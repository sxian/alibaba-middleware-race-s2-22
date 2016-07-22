package com.alibaba.middleware.race;

import com.alibaba.middleware.race.process.QueryProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by sxian.wang on 2016/7/22.
 */
public class QueryProcessTest {

    public static QueryProcessor queryProcessor;

    public static void main(String[] args) throws IOException {
        queryProcessor = new QueryProcessor();
    }

    public static void testQuery(String name, ArrayList<String> list) throws IOException {
        System.out.println("start test: " + name);
        Random random = new Random();
        long start = System.currentTimeMillis();
        int i = 0;
        int size = list.size();
        while (i++<1000000) {
            queryProcessor.queryOrder(list.get(random.nextInt(size)));
        }
        System.out.println("Test"+name+" useTime: " + (System.currentTimeMillis() - start));
    }

    public static ArrayList<String> buildQueryCondition(List<String> files) throws IOException {
        ArrayList<String> list = new ArrayList<>();
        for (String file : files) {
            BufferedReader br = Utils.createReader(file);
            String str = br.readLine();
            while (str!=null) {
                list.add(str.split("\t")[0]);
                str = br.readLine();
            }
        }
        return list;
    }
}
