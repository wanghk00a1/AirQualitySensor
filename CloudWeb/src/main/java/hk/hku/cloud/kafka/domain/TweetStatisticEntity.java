package hk.hku.cloud.kafka.domain;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author: LexKaing
 * @create: 2019-06-28 21:47
 * @description:
 **/
public class TweetStatisticEntity implements RowMapper<TweetStatisticEntity> {
    private String city;
    private String timestamp;
    private int positive;
    private int negative;
    private int total;
    private int w_positive;
    private int w_negative;
    private int w_total;
    private double random_tree;

    public double getRandom_tree() {
        return random_tree;
    }

    public void setRandom_tree(double random_tree) {
        this.random_tree = random_tree;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
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

    @Override
    public TweetStatisticEntity mapRow(ResultSet rs, int i) throws SQLException {
        TweetStatisticEntity tmp = new TweetStatisticEntity();
        tmp.setCity(rs.getString("city"));
        tmp.setTimestamp(rs.getString("timestamp"));
        tmp.setPositive(rs.getInt("positive"));
        tmp.setNegative(rs.getInt("negative"));
        tmp.setTotal(rs.getInt("total"));
        tmp.setW_positive(rs.getInt("w_positive"));
        tmp.setW_negative(rs.getInt("w_negative"));
        tmp.setW_total(rs.getInt("w_total"));
        tmp.setRandom_tree(rs.getDouble("random_tree"));
        return tmp;
    }
}