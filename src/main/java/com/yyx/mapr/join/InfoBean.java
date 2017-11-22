package com.yyx.mapr.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {
    private int orderId;
    private String dateString;
    private int pId;
    private int amount;
    private String pName;
    private int categoryId;
    private float price;
    //0订单表
    //1产品
    private int flag;

    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeUTF(dateString);
        out.writeInt(pId);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeInt(categoryId);
        out.writeFloat(price);
        out.writeInt(flag);
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "InfoBean{" +
                "orderId=" + orderId +
                ", dateString='" + dateString + '\'' +
                ", pId=" + pId +
                ", amount=" + amount +
                ", pName='" + pName + '\'' +
                ", categoryId=" + categoryId +
                ", price=" + price +
                ", flag=" + flag +
                '}';
    }

    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        dateString = in.readUTF();
        pId = in.readInt();
        amount = in.readInt();
        pName = in.readUTF();
        categoryId = in.readInt();
        price = in.readFloat();
        flag = in.readInt();
    }




    public void set (int orderId, String dateString, int pId, int amount, String pName, int categoryId, float price,int flag) {
        this.orderId = orderId;
        this.dateString = dateString;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    public InfoBean() {
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public int getpId() {
        return pId;
    }

    public void setpId(int pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    
}
