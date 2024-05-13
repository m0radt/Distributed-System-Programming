package com.assi3;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class TreeNode<T>  {

    public T data;
    public TreeNode<T> parent;
    public  List<TreeNode<T>> children;
    boolean visited;
    public TreeNode(T data) {
        this.data = data;
        this.children = new LinkedList<TreeNode<T>>();
        this.parent=null;
    }

    public TreeNode<T> addChild(T child) {
        TreeNode<T> childNode = new TreeNode<T>(child);
        childNode.parent = this;
        this.children.add(childNode);
        return childNode;
    }

    void addParent(TreeNode<T> parent) {
        parent.children.add(this);
        this.parent=parent;
        return;
    }
    void print_tree()
    {

        Queue<TreeNode<T>> tmp=new LinkedList<>();
        tmp.add(this);
        Queue<TreeNode<T>> tmp2=new LinkedList<>();

        int i=0;
        while(!tmp.isEmpty()||!tmp2.isEmpty())
        {
            if(i%2==0)
            {
                print_tree_helper(tmp, tmp2, i);
            }
            else
            {
                print_tree_helper(tmp2, tmp, i);
            }
            i++;
        }
    }

    private void print_tree_helper(Queue<TreeNode<T>> tmp, Queue<TreeNode<T>> tmp2, int i) {
        while(!tmp.isEmpty()) {
            TreeNode<T> cur = tmp.remove();
            for (TreeNode<T> node : cur.children) {
                tmp2.add(node);
            }
        }
    }


}