/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

/**
 *
 * @author cloudera
 */
public class FilterData {
    private int pathId;
    private long timestamp;
    private int time;
    private int count;
    private double speed;
    private int day;
    private int month;
    private int year;
    private int medianSpeed;
    private int maxSpeed;
    private int minSpeed;

    public FilterData() {
    }
    

    public int getPathId() {
        return pathId;
    }

    public void setPathId(int pathId) {
        this.pathId = pathId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMedianSpeed() {
        return medianSpeed;
    }

    public void setMedianSpeed(int medianSpeed) {
        this.medianSpeed = medianSpeed;
    }

    public int getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(int maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public int getMinSpeed() {
        return minSpeed;
    }

    public void setMinSpeed(int minSpeed) {
        this.minSpeed = minSpeed;
    }

    @Override
    public String toString() {
        return "FilterData{" + "pathId=" + pathId + ", timestamp=" + timestamp + ", time=" + time + ", count=" + count + ", speed=" + speed + ", day=" + day + ", month=" + month + ", year=" + year + ", medianSpeed=" + medianSpeed + ", maxSpeed=" + maxSpeed + ", minSpeed=" + minSpeed + '}';
    }
    
    
    
    
    
}
