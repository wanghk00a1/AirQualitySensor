package hk.hku.cloud.kafka.domain;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author: LexKaing
 * @create: 2019-06-29 13:58
 * @description:
 **/
public class AqiEntity implements RowMapper<AqiEntity> {
    private String city;
    private String timestamp;
    private int aqi;

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

    public int getAqi() {
        return aqi;
    }

    public void setAqi(int aqi) {
        this.aqi = aqi;
    }

    @Override
    public AqiEntity mapRow(ResultSet rs, int i) throws SQLException {
        AqiEntity tmp = new AqiEntity();
        tmp.setCity(rs.getString("city"));
        tmp.setTimestamp(rs.getString("timestamp"));
        tmp.setAqi(rs.getInt("aqi"));
        return tmp;
    }
}