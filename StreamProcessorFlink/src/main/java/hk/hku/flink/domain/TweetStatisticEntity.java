package hk.hku.flink.domain;

/**
 * @author: LexKaing
 * @create: 2019-06-28 21:47
 * @description:
 **/
public class TweetStatisticEntity {
    private String city;
    private long timestamp;
    private int positive;
    private int negative;
    private int total;
    private int w_positive;
    private int w_negative;
    private int w_total;


    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getPositive() {
        return positive;
    }

    public void setPositive(int positive) {
        this.positive = positive;
    }

    public int getNegative() {
        return negative;
    }

    public void setNegative(int negative) {
        this.negative = negative;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getW_positive() {
        return w_positive;
    }

    public void setW_positive(int w_positive) {
        this.w_positive = w_positive;
    }

    public int getW_negative() {
        return w_negative;
    }

    public void setW_negative(int w_negative) {
        this.w_negative = w_negative;
    }

    public int getW_total() {
        return w_total;
    }

    public void setW_total(int w_total) {
        this.w_total = w_total;
    }
}