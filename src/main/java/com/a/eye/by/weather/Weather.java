package com.a.eye.by.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Weather implements WritableComparable<Weather> {

    private int year;
    private int month;
    private double hot;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getHot() {
        return hot;
    }

    public void setHot(double hot) {
        this.hot = hot;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeDouble(hot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.hot = in.readDouble();
    }

    @Override
    public int compareTo(Weather o) {
        int y = Integer.compare(year, o.getYear());
        if (y == 0) {
            int m = Integer.compare(month, o.getMonth());
            if (m == 0) {
                return Double.compare(hot, o.getHot());
            } else {
                return m;
            }
        } else {
            return y;
        }
    }
}
