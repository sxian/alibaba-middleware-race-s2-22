package com.alibaba.middleware.race;

import com.alibaba.middleware.race.util.Utils;
import org.omg.SendingContext.RunTime;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alibaba.middleware.race.RaceConfig.DATA_ROOT;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static float M = 1024 * 1024;
    public static void main(String[] args) throws IOException {
//        RandomAccessFile raf = new RandomAccessFile("t/index/goodid_orderid","r");
        RandomAccessFile raf = new RandomAccessFile("t/index/buyerid_create_order","r");
//
//        long leg = raf.length();
//        raf.seek(8667668); // ap-804e-6f1c7d177abd,25468,470
//        byte[] bytes = new byte[714];
//        raf.read(bytes);
//        System.out.println(new String(bytes));
        Utils.spilitCaseFile(DATA_ROOT+"case.0",RaceConfig.STORE_PATH,10);

    }



}
