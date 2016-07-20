package com.alibaba.middleware.race.datastruct;

/**
 * descption: B+树
 *
 * @author wangwenfeng
 * @date 2016-07-20 14:42
 * @email sxian.wang@gmail.com
 */

import com.alibaba.middleware.race.util.ObjectSize;

import java.util.Random;

public class BplusTree implements BTree {

    /** 根节点 */
    protected Node root;

    /** 阶数，M值 */
    protected int rank;

    /** 叶子节点的链表头*/
    protected Node head;

    public Node getHead() {
        return head;
    }

    public void setHead(Node head) {
        this.head = head;
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int order) {
        this.rank = order;
    }

    @Override
    public RecordIndex get(Comparable key) {
        return (RecordIndex) root.get(key);
    }

    @Override
    public void remove(Comparable key) {
        root.remove(key, this);
    }

    @Override
    public void insertOrUpdate(Comparable key, RecordIndex rindex) {
        root.insertOrUpdate(key, rindex, this);
    }

    public BplusTree(int rank){
        if (rank < 3) {
            throw new RuntimeException("the tree's rank must bigger than or equals 3");
        }
        this.rank = rank;
        root = new Node(true, true);
        head = root;
    }

    //测试
//    public static void main(String[] args) {
//        BplusTree tree = new BplusTree(10);
//        Random random = new Random();
//        long current = System.currentTimeMillis();
//        for (int j = 0; j < 10000000; j++) {
//            tree.insertOrUpdate(j, new RecordIndex("a",j));
//        }
//
//        long duration = System.currentTimeMillis() - current;
//        System.out.println("time elpsed for duration: " + duration);
//        int search = 80;
//        current = System.currentTimeMillis();
//        System.out.print(tree.get(search));
//        duration = System.currentTimeMillis() - current;
//        System.out.println("time elpsed for duration: " + duration);
//    }

}

