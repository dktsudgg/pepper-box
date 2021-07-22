package com.gslab.pepper;

import java.io.Serializable;

public class CheckValue implements Serializable {

    private int checkpointId;
    private double rawData;
    private String data;
    private int alarmCount;
    private long time;

    public int getCheckpointId() {
        return checkpointId;
    }

    public double getRawData() {
        return rawData;
    }

    public String getData() {
        return data;
    }

    public int getAlarmCount() {
        return alarmCount;
    }

    public long getTime() {
        return time;
    }

    public void setCheckpointId(int checkpointId) {
        this.checkpointId = checkpointId;
    }

    public void setRawData(double rawData) {
        this.rawData = rawData;
    }

    public void setData(String data) {
        this.data = data;
    }

    public void setAlarmCount(int alarmCount) {
        this.alarmCount = alarmCount;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "CheckValue{" +
                "checkpointId=" + checkpointId +
                ", rawData=" + rawData +
                ", data='" + data + '\'' +
                ", alarmCount=" + alarmCount +
                ", time=" + time +
                '}';
    }
}
