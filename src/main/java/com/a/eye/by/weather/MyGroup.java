package com.a.eye.by.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {

    public MyGroup() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        int y = Integer.compare(w1.getYear(), w2.getYear());
        if (y == 0) {
            return Integer.compare(w1.getMonth(), w2.getMonth());
        } else {
            return y;
        }
    }

}
