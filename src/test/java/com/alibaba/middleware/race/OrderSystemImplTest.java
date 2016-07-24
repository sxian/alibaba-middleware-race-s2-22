package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.BuyerTable;
import com.alibaba.middleware.race.db.GoodsTable;
import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.db.Table;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImplTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        String[]  orderFiles = new String[]{RaceConfig.DATA_ROOT+"order.0.0", RaceConfig.DATA_ROOT+"order.0.3",
                RaceConfig.DATA_ROOT+"order.1.1", RaceConfig.DATA_ROOT+"order.2.2"};

        String[] buyerFiles = new String[]{RaceConfig.DATA_ROOT+"buyer.0.0", RaceConfig.DATA_ROOT+"buyer.1.1"};

        String[] goodsFiles = new String[]{RaceConfig.DATA_ROOT+"good.0.0", RaceConfig.DATA_ROOT+"good.1.1",
                RaceConfig.DATA_ROOT+"good.2.2"};

        String[] storeFiles = new String[]{"t/index"};

        String caseFile = RaceConfig.DATA_ROOT+"case.0";

        OrderSystemImpl orderSystem = new OrderSystemImpl();

        List<String> orderList = Arrays.asList(orderFiles);
        List<String> buyerList = Arrays.asList(buyerFiles);
        List<String> goodsList = Arrays.asList(goodsFiles);
        List<String> storeList = Arrays.asList(storeFiles);

        long start = System.currentTimeMillis();
        orderSystem.construct(orderList, buyerList, goodsList, storeList);
        System.out.println("Build useTime: " + (System.currentTimeMillis() - start));

        OrderTable orderTable = new OrderTable();
        BuyerTable buyerTable = new BuyerTable();
        GoodsTable goodsTable = new GoodsTable();
        start = System.currentTimeMillis();
        Random random = new Random();

        testSelectOrderIDByBuyerID("ap-8079-5790062e830f",random,"623405052,1476705025 592764280,1471330188 608444843,1474205376 607170153,1473561266 589987604,1469923858 607994110,1473980848 624329339,1477173088 604963484,1472441767 625080228,1477552336 611281909,1475631532 627640268,1478833461 590933084,1470401749 592531548,1471209859 610217273,1475094457 607919962,1473943204 587901373,1468865355 606649272,1473296439 626308168,1478168803 590147332,1470004468 612167404,1476076571",orderTable);
        testSelectOrderIDByBuyerID("ap-804e-6f1c7d177abd",random,"613167660,1476583518 593541005,1471721281 590387354,1470124475 609582059,1474771549 591752803,1470815362 588177611,1469005483 591127800,1470498555 607038679,1473494837 589731524,1469792912 623368567,1476685490 589997916,1469929308 626102615,1478065217 610466877,1475221330 589604286,1469727672 609393024,1474678596 606561415,1473251772 588782554,1469312108 608524333,1474245172 611267593,1475623909 625747635,1477884187 611896796,1475940675 608668839,1474317074 603958573,1471930170 605680476,1472804320 609159339,1474562561 609203453,1474584148 607682150,1473822519 607823801,1473894453 610737514,1475356289 610630371,1475302157 625544931,1477783414 590053894,1469957962 606859905,1473403879 606916278,1473433802 627801662,1478913117 627833003,1478928822 624133457,1477075771 612777323,1476388434 592383967,1471134881 625452672,1477736006 612877983,1476438695 590214923,1470037968 590549933,1470205728 626123066,1478075673 610074861,1475020268 627732603,1478878382",orderTable);
        testSelectOrderIDByBuyerID("ap-8099-40e2ca646531",random,"588895836,1469369288 624843290,1477433198 593524425,1471712408 623526128,1476766676 605297118,1472609760 605196600,1472560039 588208858,1469021483 609633361,1474797451 588505193,1469170870 607977254,1473971904 610447700,1475211595 624353004,1477184709 589596384,1469723495 591260305,1470565293 610936975,1475456182 625240440,1477631605 611907382,1475946647 590763745,1470314015 591438755,1470655736 624185609,1477100906 591003845,1470436424 591014339,1470441799 592584803,1471237526 625472744,1477746326 591645137,1470760914 592969793,1471433860 587866193,1468847774 612234169,1476110787 624502605,1477260505 626132091,1478080239 627519904,1478772648 605880059,1472904641 592065632,1470973595",orderTable);

        testSelectOrderIDByBuyerID("tp-b94a-0b98184caddc",random,"588957505,1469401488 609600546,1474780874 589254195,1469551482 612885003,1476441845 608068807,1474018221 604987277,1472453786 604890335,1472403663 591842742,1470860469 609170542,1474567979 624617126,1477318378 611782270,1475884711 624652413,1477337053 592909505,1471402330 610292570,1475133167 606615856,1473279393 587882820,1468855964 587896458,1468862460 624786513,1477404996 587927172,1468878522 604748898,1472330373 627155836,1478593249 589806964,1469830225 612771829,1476385645 625409059,1477714940 592426386,1471155864 612700850,1476349701 613121665,1476559866 590641983,1470252000 590664113,1470263325 590665185,1470263883 612546184,1476270787 592059557,1470970396",orderTable);
        testSelectOrderIDByBuyerID("tp-b95d-14e46a73038c",random,"624830861,1477427176 592773933,1471334920 624886385,1477455229 613241943,1476621030 623517462,1476762229 604026731,1471963558 589921566,1469889169 623323922,1476662290 588218758,1469026438 607292666,1473624665 591259519,1470564972 593632230,1471767265 608549639,1474257682 593704360,1471804511 626245835,1478137765 626329975,1478181008 609727087,1474844620 627233418,1478631708 593357404,1471628809 593087546,1471493309 590164204,1470012854 606177475,1473056861 588333650,1469084640 627531162,1478778275 612531582,1476263596",orderTable);
        testSelectOrderIDByBuyerID("tp-b9a0-8a786e0a66a7",random,"606178983,1473057657 590539243,1470200200 613145824,1476572366 613149193,1476574082 623421132,1476713069 590314155,1470088441 588925841,1469384691 613174535,1476586794 588931165,1469387470 623967025,1476990290 623979760,1476996327 589339257,1469593435 590359765,1470111370 612423635,1476207557 606358843,1473149284 607399349,1473678716 606782637,1473364628 592802920,1471350388 623478065,1476742287 592189062,1471034896 590395707,1470128434 625047476,1477535607 606405430,1473172364 590408353,1470134893 606287206,1473113833 624942427,1477483130 605174092,1472548557 627026357,1478528483 588114861,1468974034 605317144,1472620629 605317767,1472620998 605321922,1472623094 627041293,1478535508 588160109,1468996240 589685099,1469769200 607011151,1473481446 605495834,1472711236 589723985,1469789107 607050742,1473500713 626075424,1478051351 626589302,1478309103 611575567,1475777864 604126327,1472014816 591179317,1470525023 626610982,1478320446 604143144,1472023203 588534020,1469185730 607230690,1473593001 610474006,1475225021 604785740,1472348903 591206761,1470538148 588544835,1469190818 607247505,1473601692 593160933,1471530761 604940086,1472429721 608043796,1474005084 607289947,1473623240 624390942,1477203613 604223877,1472063493 609378896,1474671693 604234254,1472068602 604881086,1472398431 604885836,1472401155 611234244,1475606897 610969993,1475473222 612268572,1476126919 627561719,1478794043 610981214,1475478809 593905438,1471903369 608536297,1474251766 607509062,1473734534 625119233,1477571846 593918931,1471909945 593641912,1471772521 609046883,1474505875 611285557,1475633565 588829142,1469335392 591386565,1470629619 611035102,1475505605 625171877,1477597521 625813961,1477917840 589503646,1469676065 607579673,1473769336 611962605,1475974046 591444765,1470658984 611727359,1475856086 624169697,1477093034 605685505,1472806842 592499770,1471193563 609161264,1474563542 592638260,1471265643 626780812,1478405965 626781189,1478406148 592644035,1471268636 590442900,1470152358 626805613,1478416803 626814623,1478421483 592548933,1471218597 627332225,1478680079 609216135,1474590425 626835429,1478431298 626838448,1478432855 591019785,1470443997 607815509,1473890124 624660532,1477341570 592583447,1471236831 592587291,1471239130 624269252,1477142395 626866071,1478446678 624680618,1477351300 611850995,1475918158 591587637,1470732327 591594866,1470736042 610730450,1475352944 625513032,1477767667 625514367,1477768197 588412489,1469124481 591625720,1470751040 593429960,1471664183 592891541,1471393790 610634373,1475304172 592908052,1471401580 588443853,1469140260 587790286,1468808913 591687621,1470782738 588493727,1469165027 605932745,1472932755 605934278,1472933557 590042011,1469952281 587852944,1468840729 626253312,1478141023 605954341,1472943628 605992250,1472963279 624767213,1477394641 624778318,1477400607 587923203,1468876699 626444724,1478236121 626462574,1478245269 611606368,1475793429 606921492,1473436542 587981202,1468905567 606933599,1473442721 627174639,1478602299 593299790,1471599498 627182180,1478606073 611677363,1475830147 610358047,1475166964 611684091,1475833759 593045671,1471471480 626698890,1478364677 609744993,1474853036 593334648,1471616755 612039841,1476013014 609758176,1474859353 593064884,1471481778 627880636,1478953483 593350002,1471624578 612052754,1476019795 627900549,1478963859 624150536,1477083801 589789426,1469821506 624165274,1477090891 623784729,1476898148 589836781,1469845382 623813619,1476912497 606102581,1473019341 605090258,1472505548 612664198,1476330854 612827630,1476413324 611442866,1475711476 610031014,1474997176 623879749,1476946348 612712235,1476355305 590190820,1470025314 613012743,1476505168 623586103,1476796834 612731089,1476364728 604398054,1472150489 588248765,1469041652 613041928,1476519919 613090549,1476544717 588308327,1469071513 604501299,1472204229 588362051,1469099299 588362348,1469099529 588364405,1469100487 588678946,1469260132 590603053,1470233232 588716179,1469278126 624518695,1477268193 588749670,1469295665 627667011,1478847061 627414765,1478720078 611128524,1475552972 627420627,1478723027 610113327,1475040090 610115448,1475041307 627461591,1478743602 626187969,1478108516 611177636,1475577496 626227553,1478128664 604301250,1472101400 612541967,1476268600 612609888,1476304189",orderTable);

        long end = System.currentTimeMillis();
        System.out.println("use Time:" + (end-start));

        ArrayList<OrderSystemImpl.Row> orderRows = buildQueryList(orderFiles);
        System.out.println("build order data complete, start query...");
        ArrayList<OrderSystemImpl.Row> buyerRows = buildQueryList(buyerFiles);
        System.out.println("build buyer data complete, start query...");
        ArrayList<OrderSystemImpl.Row> goodsRows = buildQueryList(goodsFiles);
        System.out.println("build goods data complete, start query...");

        start = System.currentTimeMillis();
        HashMap<String,OrderSystemImpl.Row> orderMap = buildQueryMap(orderTable,orderRows,"orderid");
        System.out.println("query order complete, useTime: "+(System.currentTimeMillis() - start) );
        System.out.println("start count order success...");
        countSuccess(orderRows, orderMap,"orderid");
        System.out.println("****************************************");

        start = System.currentTimeMillis();
        HashMap<String,OrderSystemImpl.Row> buyerMap = buildQueryMap(buyerTable,buyerRows,"buyerid");
        System.out.println("query buyer complete, useTime: "+(System.currentTimeMillis() - start) );
        System.out.println("start count buyer success...");
        countSuccess(buyerRows, buyerMap,"buyerid");
        System.out.println("****************************************");

        start = System.currentTimeMillis();
        HashMap<String,OrderSystemImpl.Row> goodsMap = buildQueryMap(goodsTable,goodsRows,"goodid");
        System.out.println("query goods complete, useTime: "+(System.currentTimeMillis() - start) );
        System.out.println("start count goods success...");
        countSuccess(goodsRows, goodsMap,"goodid");

    }

    public static ArrayList<OrderSystemImpl.Row> buildQueryList(String[] files) throws IOException {
        ArrayList<OrderSystemImpl.Row> rows = new ArrayList<>();

        for (String file : files) {
            BufferedReader br = Utils.createReader(file);
            String str = br.readLine();
            while (str!=null) {
                OrderSystemImpl.Row row = OrderSystemImpl.createRow(str);
                rows.add(row);
                str = br.readLine();
            }
        }
        return rows;
    }

    public static HashMap<String,OrderSystemImpl.Row> buildQueryMap(Table table, List<OrderSystemImpl.Row> rows, String id) {
        HashMap<String,OrderSystemImpl.Row> map =new HashMap<>();
        for (int i = 0;i<rows.size();i++) {
            String key = rows.get(i).get(id).valueAsString();
            OrderSystemImpl.Row row = table.selectRowById(key);
            if (row!=null) {
                map.put(key,row);
            }
        }
        return map;
    }

    public static void testSelectOrderIDByBuyerID(String id, Random random, String strs,OrderTable table) {
        System.out.println("**************");
        TreeMap<String, String> map = new TreeMap<>();
        for (String str : strs.split(" ")) {
            String[] kv = str.split(",");
            map.put(kv[1],kv[0]); // 时间 订单号
        }
        List<String> keyList = new ArrayList<>(map.keySet());
        int s = random.nextInt(keyList.size()-1);
        int e = random.nextInt(keyList.size()-1);
        while (s==e || s>e) {
            if (s==e) {
                s = random.nextInt(keyList.size()-1);
                e = random.nextInt(keyList.size()-1);
            } else {
                int tmp = e;
                e = s;
                s = tmp;
            }
        }
        long start = Long.valueOf(keyList.get(s));
        long end = Long.valueOf(keyList.get(e));

        List<String> list = table.selectOrderIDByBuyerID(id,start,end);
        System.out.println("query result count: " + list.size()+", should: "+(e-s));
        for (int i = 0;i<e-s;i++) {
            System.out.println("------------");
            System.out.println("time is:"+keyList.get(s+i)+", result should: " +map.get(keyList.get(s+i))+", fact: "+
                    list.get(i)+ ". result is: "+map.get(keyList.get(s+i)).equals(list.get(i).split(",")[1]));
        }
    }
    public static void countSuccess(List<OrderSystemImpl.Row> rows, Map<String,OrderSystemImpl.Row> map,String id) {
        int find = 0;
        int matched = 0;
        for (int i = 0;i<rows.size();i++) {
            OrderSystemImpl.Row row = rows.get(i);
            OrderSystemImpl.Row _row = map.get(row.get(id).valueAsString());
            if (_row!=null) {
                find++;
                Set<Map.Entry<String, OrderSystemImpl.KV>> entrySet = row.entrySet();
                boolean ok = true;
                for (Map.Entry<String, OrderSystemImpl.KV> entry : entrySet) {
                    try {
                        OrderSystemImpl.KV kv = _row.getKV(entry.getKey());
                    } catch (Exception e) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    matched++;
                } else {
                    int a = 5;
                }
            }
        }
        System.out.println("find data: "+ find+"\nmatched data: "+matched);
    }
    public void ProcessCase(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(RaceConfig.DATA_ROOT+"order.2.2")));
        String record = br.readLine();
        while (record!=null) {
            if ((record.equals("}")||record.equals(""))) { // todo 执行查询
                record = br.readLine();
                continue;
            }

            String[] kv = record.split(":");
            switch (kv[0]) {
                case "CASE":

                    break;
                case "ORDERID":
                    break;
                case "SALERID":
                    break;
                case "GOODID":
                    break;
                case "KEYS":
                    break;
                case "STARTTIME":
                    break;
                case "ENDTIME":
                    break;
                case "Result":
                    break;
            }
            record = br.readLine();
        }
        br.close();
    }

}
