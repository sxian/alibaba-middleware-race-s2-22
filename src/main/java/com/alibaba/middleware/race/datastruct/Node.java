package com.alibaba.middleware.race.datastruct;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * descption: B+树节点
 *
 * @author wangwenfeng
 * @date 2016-07-20 14:46
 * @email sxian.wang@gmail.com
 */

public class Node {

    /** 是否为叶子节点 */
    protected boolean isLeaf;

    /** 是否为根节点*/
    protected boolean isRoot;

    /** 父节点 */
    protected Node parent;

    /** 叶节点的前节点*/
    protected Node previous;

    /** 叶节点的后节点*/
    protected Node next;

    /** 节点的关键字 */
    protected List<Entry<Comparable, String>> entries;

    /** 子节点 */
    protected List<Node> children;

    /* 存储长度 */
    protected int length;

    /* 文件位置偏移量 -> 起始处 */
    long pos;

    /* 未输出raw data时的长度，即起始*/
    long startPos;

    /* 是否已经toString*/
    private boolean isToString = false;
    private String _string;

    public Node(boolean isLeaf) {
        this.isLeaf = isLeaf;
        entries = new ArrayList<>();

        if (!isLeaf) {
            children = new ArrayList<>();
        }
    }

    public Node(boolean isLeaf, boolean isRoot) {
        this(isLeaf);
        this.isRoot = isRoot;
    }

    public String get(Comparable key) {
        //如果是叶子节点
        if (isLeaf) {
            for (Entry<Comparable, String> entry : entries) {
                if (entry.getKey().compareTo(key) == 0) {
                    return entry.getValue();
                }
            }

            return null;
        }else {
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                return children.get(0).get(key);
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                return children.get(children.size()-1).get(key);
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        return children.get(i).get(key);
                    }
                }
            }
        }

        return null;
    }

    public void insertOrUpdate(Comparable key, String obj, BplusTree tree){
        //如果是叶子节点
        if (isLeaf){
            //不需要分裂，直接插入或更新
            if (entries.size() < tree.getRank()){ // todo 按时间戳排序存在相等的情况 不要update
                insertOrUpdate(key, obj);
                if (parent != null) {
                    //更新父节点
                    parent.updateInsert(tree);
                }

                //需要分裂
            }else {
                //分裂成左右两个节点
                Node left = new Node(true);
                Node right = new Node(true);
                //设置链接
                if (previous != null){
                    previous.setNext(left);
                    left.setPrevious(previous);
                }
                if (next != null) {
                    next.setPrevious(right);
                    right.setNext(next);
                }
                if (previous == null){
                    tree.setHead(left);
                }

                left.setNext(right);
                right.setPrevious(left);
                previous = null;
                next = null;

                //左右两个节点关键字长度
                int leftSize = (tree.getRank() + 1) / 2 + (tree.getRank() + 1) % 2;
                int rightSize = (tree.getRank() + 1) / 2;
                //复制原节点关键字到分裂出来的新节点
                insertOrUpdate(key, obj);
                for (int i = 0; i < leftSize; i++){
                    left.getEntries().add(entries.get(i));
                }
                for (int i = 0; i < rightSize; i++){
                    right.getEntries().add(entries.get(leftSize + i));
                }

                //如果不是根节点
                if (parent != null) {
                    //调整父子节点关系
                    int index = parent.getChildren().indexOf(this);
                    parent.getChildren().remove(this);
                    left.setParent(parent);
                    right.setParent(parent);
                    parent.getChildren().add(index,left);
                    parent.getChildren().add(index + 1, right);
                    setEntries(null);
                    setChildren(null);

                    //父节点插入或更新关键字
                    parent.updateInsert(tree);
                    setParent(null);
                    //如果是根节点
                }else {
                    isRoot = false;
                    Node parent = new Node(false, true);
                    tree.setRoot(parent);
                    left.setParent(parent);
                    right.setParent(parent);
                    parent.getChildren().add(left);
                    parent.getChildren().add(right);
                    setEntries(null);
                    setChildren(null);

                    //更新根节点
                    parent.updateInsert(tree);
                }


            }

            //如果不是叶子节点
        }else {
            //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                children.get(0).insertOrUpdate(key, obj, tree);
                //如果key大于节点最右边的key，沿最后一个子节点继续搜索
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                children.get(children.size()-1).insertOrUpdate(key, obj, tree);
                //否则沿比key大的前一个子节点继续搜索
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        children.get(i).insertOrUpdate(key, obj, tree);
                        break;
                    }
                }
            }
        }
    }

    /** 插入节点后中间节点的更新 */
    protected void updateInsert(BplusTree tree){

        validate(this, tree);

        //如果子节点数超出阶数，则需要分裂该节点
        if (children.size() > tree.getRank()) {
            //分裂成左右两个节点
            Node left = new Node(false);
            Node right = new Node(false);
            //左右两个节点关键字长度
            int leftSize = (tree.getRank() + 1) / 2 + (tree.getRank() + 1) % 2;
            int rightSize = (tree.getRank() + 1) / 2;
            //复制子节点到分裂出来的新节点，并更新关键字
            for (int i = 0; i < leftSize; i++){
                left.getChildren().add(children.get(i));
                left.getEntries().add(new SimpleEntry<Comparable, String>(children.get(i).getEntries().get(0).getKey(), null));
                children.get(i).setParent(left);
            }
            for (int i = 0; i < rightSize; i++){
                right.getChildren().add(children.get(leftSize + i));
                right.getEntries().add(new SimpleEntry<Comparable, String>(children.get(leftSize + i).getEntries().get(0).getKey(), null));
                children.get(leftSize + i).setParent(right);
            }

            //如果不是根节点
            if (parent != null) {
                //调整父子节点关系
                int index = parent.getChildren().indexOf(this);
                parent.getChildren().remove(this);
                left.setParent(parent);
                right.setParent(parent);
                parent.getChildren().add(index,left);
                parent.getChildren().add(index + 1, right);
                setEntries(null);
                setChildren(null);

                //父节点更新关键字
                parent.updateInsert(tree);
                setParent(null);
                //如果是根节点
            }else {
                isRoot = false;
                Node parent = new Node(false, true);
                tree.setRoot(parent);
                left.setParent(parent);
                right.setParent(parent);
                parent.getChildren().add(left);
                parent.getChildren().add(right);
                setEntries(null);
                setChildren(null);

                //更新根节点
                parent.updateInsert(tree);
            }
        }
    }

    /** 调整节点关键字*/
    protected static void validate(Node node, BplusTree tree) {

        // 如果关键字个数与子节点个数相同
        if (node.getEntries().size() == node.getChildren().size()) {
            for (int i = 0; i < node.getEntries().size(); i++) {
                Comparable key = node.getChildren().get(i).getEntries().get(0).getKey();
                if (node.getEntries().get(i).getKey().compareTo(key) != 0) {
                    node.getEntries().remove(i);
                    node.getEntries().add(i, new SimpleEntry<Comparable, String>(key, null));
                    if(!node.isRoot()){
                        validate(node.getParent(), tree);
                    }
                }
            }
            // 如果子节点数不等于关键字个数但仍大于M / 2并且小于M，并且大于2
        } else if (node.isRoot() && node.getChildren().size() >= 2
                ||node.getChildren().size() >= tree.getRank() / 2
                && node.getChildren().size() <= tree.getRank()
                && node.getChildren().size() >= 2) {
            node.getEntries().clear();
            for (int i = 0; i < node.getChildren().size(); i++) {
                Comparable key = node.getChildren().get(i).getEntries().get(0).getKey();
                node.getEntries().add(new SimpleEntry<Comparable, String>(key, null));
                if (!node.isRoot()) {
                    validate(node.getParent(), tree);
                }
            }
        }
    }

    /** 删除节点后中间节点的更新*/
    protected void updateRemove(BplusTree tree) {

        validate(this, tree);

        // 如果子节点数小于M / 2或者小于2，则需要合并节点
        if (children.size() < tree.getRank() / 2 || children.size() < 2) {
            if (isRoot) {
                // 如果是根节点并且子节点数大于等于2，OK
                if (children.size() >= 2) {
                    return;
                    // 否则与子节点合并
                } else {
                    Node root = children.get(0);
                    tree.setRoot(root);
                    root.setParent(null);
                    root.setRoot(true);
                    setEntries(null);
                    setChildren(null);
                }
            } else {
                //计算前后节点
                int currIdx = parent.getChildren().indexOf(this);
                int prevIdx = currIdx - 1;
                int nextIdx = currIdx + 1;
                Node previous = null, next = null;
                if (prevIdx >= 0) {
                    previous = parent.getChildren().get(prevIdx);
                }
                if (nextIdx < parent.getChildren().size()) {
                    next = parent.getChildren().get(nextIdx);
                }

                // 如果前节点子节点数大于M / 2并且大于2，则从其处借补
                if (previous != null
                        && previous.getChildren().size() > tree.getRank() / 2
                        && previous.getChildren().size() > 2) {
                    //前叶子节点末尾节点添加到首位
                    int idx = previous.getChildren().size() - 1;
                    Node borrow = previous.getChildren().get(idx);
                    previous.getChildren().remove(idx);
                    borrow.setParent(this);
                    children.add(0, borrow);
                    validate(previous, tree);
                    validate(this, tree);
                    parent.updateRemove(tree);

                    // 如果后节点子节点数大于M / 2并且大于2，则从其处借补
                } else if (next != null
                        && next.getChildren().size() > tree.getRank() / 2
                        && next.getChildren().size() > 2) {
                    //后叶子节点首位添加到末尾
                    Node borrow = next.getChildren().get(0);
                    next.getChildren().remove(0);
                    borrow.setParent(this);
                    children.add(borrow);
                    validate(next, tree);
                    validate(this, tree);
                    parent.updateRemove(tree);

                    // 否则需要合并节点
                } else {
                    // 同前面节点合并
                    if (previous != null
                            && (previous.getChildren().size() <= tree.getRank() / 2 || previous.getChildren().size() <= 2)) {

                        for (int i = previous.getChildren().size() - 1; i >= 0; i--) {
                            Node child = previous.getChildren().get(i);
                            children.add(0, child);
                            child.setParent(this);
                        }
                        previous.setChildren(null);
                        previous.setEntries(null);
                        previous.setParent(null);
                        parent.getChildren().remove(previous);
                        validate(this, tree);
                        parent.updateRemove(tree);

                        // 同后面节点合并
                    } else if (next != null
                            && (next.getChildren().size() <= tree.getRank() / 2 || next.getChildren().size() <= 2)) {

                        for (int i = 0; i < next.getChildren().size(); i++) {
                            Node child = next.getChildren().get(i);
                            children.add(child);
                            child.setParent(this);
                        }
                        next.setChildren(null);
                        next.setEntries(null);
                        next.setParent(null);
                        parent.getChildren().remove(next);
                        validate(this, tree);
                        parent.updateRemove(tree);
                    }
                }
            }
        }
    }

    public void remove(Comparable key, BplusTree tree){
        //如果是叶子节点
        if (isLeaf){

            //如果不包含该关键字，则直接返回
            if (!contains(key)){
                return;
            }

            //如果既是叶子节点又是跟节点，直接删除
            if (isRoot) {
                remove(key);
            }else {
                //如果关键字数大于M / 2，直接删除
                if (entries.size() > tree.getRank() / 2 && entries.size() > 2) {
                    remove(key);
                }else {
                    //如果自身关键字数小于M / 2，并且前节点关键字数大于M / 2，则从其处借补
                    if (previous != null
                            && previous.getEntries().size() > tree.getRank() / 2
                            && previous.getEntries().size() > 2
                            && previous.getParent() == parent) {
                        int size = previous.getEntries().size();
                        Entry<Comparable, String> entry = previous.getEntries().get(size - 1);
                        previous.getEntries().remove(entry);
                        //添加到首位
                        entries.add(0, entry);
                        remove(key);
                        //如果自身关键字数小于M / 2，并且后节点关键字数大于M / 2，则从其处借补
                    }else if (next != null
                            && next.getEntries().size() > tree.getRank() / 2
                            && next.getEntries().size() > 2
                            && next.getParent() == parent) {
                        Entry<Comparable, String> entry = next.getEntries().get(0);
                        next.getEntries().remove(entry);
                        //添加到末尾
                        entries.add(entry);
                        remove(key);
                        //否则需要合并叶子节点
                    }else {
                        //同前面节点合并
                        if (previous != null
                                && (previous.getEntries().size() <= tree.getRank() / 2 || previous.getEntries().size() <= 2)
                                && previous.getParent() == parent) {
                            for (int i = previous.getEntries().size() - 1; i >=0; i--) {
                                //从末尾开始添加到首位
                                entries.add(0, previous.getEntries().get(i));
                            }
                            remove(key);
                            previous.setParent(null);
                            previous.setEntries(null);
                            parent.getChildren().remove(previous);
                            //更新链表
                            if (previous.getPrevious() != null) {
                                Node temp = previous;
                                temp.getPrevious().setNext(this);
                                previous = temp.getPrevious();
                                temp.setPrevious(null);
                                temp.setNext(null);
                            }else {
                                tree.setHead(this);
                                previous.setNext(null);
                                previous = null;
                            }
                            //同后面节点合并
                        }else if(next != null
                                && (next.getEntries().size() <= tree.getRank() / 2 || next.getEntries().size() <= 2)
                                && next.getParent() == parent){
                            for (int i = 0; i < next.getEntries().size(); i++) {
                                //从首位开始添加到末尾
                                entries.add(next.getEntries().get(i));
                            }
                            remove(key);
                            next.setParent(null);
                            next.setEntries(null);
                            parent.getChildren().remove(next);
                            //更新链表
                            if (next.getNext() != null) {
                                Node temp = next;
                                temp.getNext().setPrevious(this);
                                next = temp.getNext();
                                temp.setPrevious(null);
                                temp.setNext(null);
                            }else {
                                next.setPrevious(null);
                                next = null;
                            }
                        }
                    }
                }
                parent.updateRemove(tree);
            }
            //如果不是叶子节点
        }else {
            //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
            if (key.compareTo(entries.get(0).getKey()) <= 0) {
                children.get(0).remove(key, tree);
                //如果key大于节点最右边的key，沿最后一个子节点继续搜索
            }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
                children.get(children.size()-1).remove(key, tree);
                //否则沿比key大的前一个子节点继续搜索
            }else {
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).getKey().compareTo(key) <= 0 && entries.get(i+1).getKey().compareTo(key) > 0) {
                        children.get(i).remove(key, tree);
                        break;
                    }
                }
            }
        }
    }

    /** 判断当前节点是否包含该关键字*/
    protected boolean contains(Comparable key) {
        for (Entry<Comparable, String> entry : entries) {
            if (entry.getKey().compareTo(key) == 0) {
                return true;
            }
        }
        return false;
    }

    /** 插入到当前节点的关键字中*/
    protected void insertOrUpdate(Comparable key, String obj){
        Entry<Comparable, String> entry = new SimpleEntry<>(key, obj);
//        entries.add(entry);
//        如果关键字列表长度为0，则直接插入
        if (entries.size() == 0) {
            entries.add(entry);
            return;
        }
//        否则遍历列表
        for (int i = 0; i < entries.size(); i++) {
            //如果该关键字键值已存在，则更新
            if (entries.get(i).getKey().compareTo(key) == 0) {
                entries.get(i).setValue(obj);
                return;
                //否则插入
            }else if (entries.get(i).getKey().compareTo(key) > 0){
                //插入到链首
                if (i == 0) {
                    entries.add(0, entry);
                    return;
                    //插入到中间
                }else {
                    entries.add(i, entry);
                    return;
                }
            }
        }
        //插入到末尾
        entries.add(entries.size(), entry);
    }

    /** 删除节点*/
    protected void remove(Comparable key){
        int index = -1;
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).getKey().compareTo(key) == 0) {
                index = i;
                break;
            }
        }
        if (index != -1) {
            entries.remove(index);
        }
    }

    public Node getPrevious() {
        return previous;
    }

    public void setPrevious(Node previous) {
        this.previous = previous;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public List<Entry<Comparable, String>> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry<Comparable, String>> entries) {
        this.entries = entries;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean isRoot) {
        this.isRoot = isRoot;
    }

    public long getPos() {
        return pos;
    }

    public int getLength() {
        return length;
    }
    public long writeToDisk(long position, BufferedWriter bw) throws IOException {
        if (isLeaf) {
            startPos = position; // 没有输出前的位置
            pos = position;
            long entryLen = 0;
            StringBuilder sb = new StringBuilder();
            for (Entry<Comparable, String> entry : entries) {
                sb.append(entry.getValue());
                entryLen += entry.getValue().getBytes().length;
            }
            length = toString().getBytes().length;
            sb.append(toString());
            bw.write(sb.toString().toCharArray()); // 把数据写进去
            pos = position + entryLen;
            return entryLen+length; // 子节点只需要把entries的数据弄好就行，所以返回length
        }

        long chindernPos = 0;
        for (Node node : children) { // 是返回给上一层的所以同一级没法知道
            // 用pos会有一堆0出来 -> 忘了的话改成pos + chindernPos  当root节点的值传过来的时候，pos并没有更新为position，所以会
            // 多次出现0的错位，纯属手残
            chindernPos += node.writeToDisk(position + chindernPos,bw);
        }

        pos = position+chindernPos; // 所有的节点位置应该是子节点的长度加上上面传的长度 -> 内部节点同一级的children后面的加上前面的
        bw.write(toString().toCharArray());
        length = toString().getBytes().length;
        return chindernPos+length;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        if (!isToString) {  // todo 空间换时间 -> 确认下会不会造成额外的内存消耗，以及导致full gc
            isToString = true;
            if (isLeaf) {
                sb.append("1 ");
                int offset = 0;
                for (int i = 0;i<entries.size();i++) {
                    int rowLen = entries.get(i).getValue().toString().getBytes().length;
                    sb.append(entries.get(i).getKey()).append(",").append(pos+offset).append(",") // orderid,pos,length
                            .append(rowLen).append(" ");
                    offset += rowLen;
                }
            } else {
                sb.append("0 "); // 标记位，表示为内部节点，1为叶子节点.
                for (int i = 0;i<children.size();i++) {
                    sb.append(entries.get(i).getKey()).append(",").append(children.get(i).pos).append(",")
                            .append(children.get(i).length).append(" ");
                }
            }
            _string = sb.append("\n").toString();
        }
        return _string;
    }


}
