package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.RaceConfig;

import java.io.*;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class Utils {
    public static BufferedWriter createWriter(String file) throws IOException {
        return new BufferedWriter(new FileWriter(file));
    }

    public static BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }



    public static void createTestData() throws IOException {
        String str = " asdfghfa3erhq23425aes635tr4q456fsdfg3q7824563e9r8WE$%#T%ERQFDGWEATFGWEFQECWFasfasefge4dfW"
                +"HLEJFOheotufeoFIHpdehfOHIWOSFHspdEohrOhoSHOISJHORYHWUROqry0UJ0uO2YHR9wuyr93QYHQWY409824yQUY49yh49"
                +"iuqwg4iu2QH4GIqu24gh98qYH4RI3qyug094G9q4gt893QG48934g03Q84Giu4g9Q24Gu8i9t4gqu94G09q4gb8T49qg48QY4"
                +"VG9ub4089H4hg49QH40qyg3h4uiG3Q409gqh3p94gQU4HBq9-84gQI34BG983qtg4i3qub4iuj3qg48q3gh4iyuq3g49uq3g4"
                +"q93y409q3H4B8IYq3g49q3h498q3yh4uhq3fv4ty3d8ui4ghQ3984TRfqdtr3dxs46r3qfg49uq3ty4Q3T48QI34y093Q4GRI3"
                +"qjh4r93QUWBRV9Q03Y4TQ9U40-3Q4RHIQwejr9QWHRBQWEIKRYUQ2384HQ";

        int count = 0;
        BufferedWriter bw = createWriter(RaceConfig.STORE_PATH+"test");
        while (count++ < 10000000) {
            if (count%100000==0)
                System.out.println(count);
            bw.write(str);
        }
        bw.flush();
        bw.close();
    }
}
