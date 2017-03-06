package com.a.eye.by.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {

    public MySort() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        int y = Integer.compare(w1.getYear(), w2.getYear());
        if (y == 0) {
            int m = Integer.compare(w1.getMonth(), w2.getMonth());
            if (m == 0) {
                return Double.compare(w1.getHot(), w2.getHot());
            } else {
                return m;
            }
        } else {
            return y;
        }
    }

}
