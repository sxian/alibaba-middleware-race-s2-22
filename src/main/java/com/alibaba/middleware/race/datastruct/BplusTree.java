package com.alibaba.middleware.race.datastruct;

import com.alibaba.middleware.race.OrderSystemImpl.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * descption: B+树
 *
 * @author wangwenfeng
 * @date 2016-07-20 14:42
 * @email sxian.wang@gmail.com
 */

public class BplusTree {

    /* 根节点 */
    protected Node root;

    /* 阶数，M值 */
    protected int rank;

    /* 叶子节点的链表头*/
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

    public Row get(Comparable key) {
        return root.get(key);
    }

    public void remove(Comparable key) {
        root.remove(key, this);
    }

    public void insertOrUpdate(Comparable key, Row rindex) {
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

    public void layerTraver() {
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        TreeMap<Integer,HashMap<Integer,String>> sortedMap = new TreeMap<>();
        Object node = root;
        while (node!=null) {
            if (node instanceof Node) {
                Node _node = (Node) node;
                if (_node.isLeaf) {
                    for (Map.Entry<Comparable, Row> entry : _node.entries) {
                        queue.offer(entry.getValue());
                    }
                } else {
                    for (Node cnode : _node.children) {
                        queue.offer(cnode);
                    }
//                    _node.children.
                }
            }
            node = queue.poll();
        }
    }

}

