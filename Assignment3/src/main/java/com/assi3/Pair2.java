package com.assi3;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Pair2 implements WritableComparable<Pair2> {
    public int key;
    public int value;

    public Pair2() {}

    public Pair2(int key, int value) {
        this.key = key;
        this.value = value;
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(key);
        out.writeInt(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = in.readInt();
        value = in.readInt();
    }

    @Override
    public int compareTo(Pair2 other) {
        int cmp = Integer.compare(key, other.key);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(value, other.value);
    }

    @Override
    public String toString() {
        return "(" + key + "," + value + ")";
    }
}