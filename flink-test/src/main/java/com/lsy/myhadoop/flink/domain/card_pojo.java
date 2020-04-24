package com.lsy.myhadoop.flink.domain;

public class card_pojo {
    String busNo;
    String cardKind;
    String cardNo;
    String createTime;
    String discountAmount;
    String id;
    String lineNo;
    String modifyTime;
    String runDate;
    String runMonth;
    String txnAmount;
    String txnDate;
    String txnKind;
    String txnPrice;
    String txnType;

    public String getBusNo() {
        return busNo;
    }

    public void setBusNo(String busNo) {
        this.busNo = busNo;
    }

    public String getCardKind() {
        return cardKind;
    }

    public void setCardKind(String cardKind) {
        this.cardKind = cardKind;
    }

    public String getCardNo() {
        return cardNo;
    }

    public void setCardNo(String cardNo) {
        this.cardNo = cardNo;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getDiscountAmount() {
        return discountAmount;
    }

    public void setDiscountAmount(String discountAmount) {
        this.discountAmount = discountAmount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLineNo() {
        return lineNo;
    }

    public void setLineNo(String lineNo) {
        this.lineNo = lineNo;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getRunDate() {
        return runDate;
    }

    public void setRunDate(String runDate) {
        this.runDate = runDate;
    }

    public String getRunMonth() {
        return runMonth;
    }

    public void setRunMonth(String runMonth) {
        this.runMonth = runMonth;
    }

    public String getTxnAmount() {
        return txnAmount;
    }

    public void setTxnAmount(String txnAmount) {
        this.txnAmount = txnAmount;
    }

    public String getTxnDate() {
        return txnDate;
    }

    public void setTxnDate(String txnDate) {
        this.txnDate = txnDate;
    }

    public String getTxnKind() {
        return txnKind;
    }

    public void setTxnKind(String txnKind) {
        this.txnKind = txnKind;
    }

    public String getTxnPrice() {
        return txnPrice;
    }

    public void setTxnPrice(String txnPrice) {
        this.txnPrice = txnPrice;
    }

    public String getTxnType() {
        return txnType;
    }

    public void setTxnType(String txnType) {
        this.txnType = txnType;
    }

    @Override
    public String toString() {
        return "card_pojo{" +
                "busNo='" + busNo + '\'' +
                ", cardNo='" + cardNo + '\'' +
                ", lineNo='" + lineNo + '\'' +
                ", runDate='" + runDate + '\'' +
                '}';
    }
}
