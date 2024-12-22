package com.ververica.field.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents an event with device data.
 */
public class DeviceEvent implements Serializable {

    private String id; // Device ID
    private int action; // Action code
    private double temp; // Temperature
    private long rpm; // Rotations per minute
    private long detectionTime; // Detection timestamp in milliseconds

    public DeviceEvent() {
        // Default constructor required for Flink POJO
    }

    public DeviceEvent(String id, int action, double temp, long rpm, long detectionTime) {
        this.id = id;
        this.action = action;
        this.temp = temp;
        this.rpm = rpm;
        this.detectionTime = detectionTime;
    }

    // Getters and setters (required for Flink POJO)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public long getRpm() {
        return rpm;
    }

    public void setRpm(long rpm) {
        this.rpm = rpm;
    }

    public long getDetectionTime() {
        return detectionTime;
    }

    public void setDetectionTime(long detectionTime) {
        this.detectionTime = detectionTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeviceEvent that = (DeviceEvent) o;
        return action == that.action &&
                Double.compare(that.temp, temp) == 0 &&
                rpm == that.rpm &&
                detectionTime == that.detectionTime &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, action, temp, rpm, detectionTime);
    }

    @Override
    public String toString() {
        return "DeviceEvent{" +
                "id='" + id + '\'' +
                ", action=" + action +
                ", temp=" + temp +
                ", rpm=" + rpm +
                ", detectionTime=" + detectionTime +
                '}';
    }
}

