package com.creanga.playground.spark.example.tableformats;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Calendar;

public class Trip implements Serializable {

    private String tripId;
    private String userId;
    private String driverId;
    private double startLatitude;
    private double startLongitude;
    private double endLatitude;
    private double endLongitude;
    private long orderTimestamp;
    private long startTimestamp;
    private long endTimestamp;
    private byte paymentType;

    private int orderYear;
    private int orderMonth;
    private int orderDay;

    public Trip() {
    }

    private void setFields(){
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(orderTimestamp);
        orderYear = cal.get(Calendar.YEAR);
        orderMonth = cal.get(Calendar.MONTH);
        orderDay = cal.get(Calendar.DAY_OF_MONTH);
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        tripId = in.readUTF();
        userId = in.readUTF();
        driverId = in.readUTF();
        startLatitude = in.readDouble();
        startLongitude = in.readDouble();
        endLatitude = in.readDouble();
        endLongitude = in.readDouble();
        orderTimestamp = in.readLong();
        startTimestamp = in.readLong();
        endTimestamp = in.readLong();
        paymentType = in.readByte();
        setFields();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeUTF(tripId);
        out.writeUTF(userId);
        out.writeUTF(driverId);
        out.writeDouble(startLatitude);
        out.writeDouble(startLongitude);
        out.writeDouble(endLatitude);
        out.writeDouble(endLongitude);
        out.writeLong(orderTimestamp);
        out.writeLong(startTimestamp);
        out.writeLong(endTimestamp);
        out.writeByte(paymentType);
    }

    public Trip(String tripId, String userId, String driverId, double startLatitude, double startLongitude, double endLatitude, double endLongitude, long orderTimestamp, long startTimestamp, long endTimestamp, byte paymentType) {
        this.tripId = tripId;
        this.userId = userId;
        this.driverId = driverId;
        this.startLatitude = startLatitude;
        this.startLongitude = startLongitude;
        this.endLatitude = endLatitude;
        this.endLongitude = endLongitude;
        this.orderTimestamp = orderTimestamp;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.paymentType = paymentType;
        setFields();
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public double getStartLatitude() {
        return startLatitude;
    }

    public void setStartLatitude(double startLatitude) {
        this.startLatitude = startLatitude;
    }

    public double getStartLongitude() {
        return startLongitude;
    }

    public void setStartLongitude(double startLongitude) {
        this.startLongitude = startLongitude;
    }

    public double getEndLatitude() {
        return endLatitude;
    }

    public void setEndLatitude(double endLatitude) {
        this.endLatitude = endLatitude;
    }

    public double getEndLongitude() {
        return endLongitude;
    }

    public void setEndLongitude(double endLongitude) {
        this.endLongitude = endLongitude;
    }

    public long getOrderTimestamp() {
        return orderTimestamp;
    }

    public void setOrderTimestamp(long orderTimestamp) {
        this.orderTimestamp = orderTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public byte getPaymentType() {
        return paymentType;
    }

    public int getOrderYear() {
        return orderYear;
    }

    public int getOrderMonth() {
        return orderMonth;
    }

    public int getOrderDay() {
        return orderDay;
    }

    public void setOrderYear(int orderYear) {
        this.orderYear = orderYear;
    }

    public void setOrderMonth(int orderMonth) {
        this.orderMonth = orderMonth;
    }

    public void setOrderDay(int orderDay) {
        this.orderDay = orderDay;
    }

    public void setPaymentType(byte paymentType) {
        this.paymentType = paymentType;
    }
}
