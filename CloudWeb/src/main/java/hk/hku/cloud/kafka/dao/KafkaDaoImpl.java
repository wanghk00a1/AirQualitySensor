package hk.hku.cloud.kafka.dao;

import hk.hku.cloud.kafka.domain.TweetStatisticEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LexKaing
 * @create: 2019-06-28 22:02
 * @description:
 **/
@Repository("kafkaDao")
public class KafkaDaoImpl {

    @Autowired
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
    }

    public int insertAqi(TweetStatisticEntity entity) {
        String sql = "insert into t_aqi_data(city,timestamp,positive,negative,total,w_positive,w_negative,w_total) "
                + "values(:city,:timestamp,:positive,:negative,:total,:w_positive,:w_negative,:w_total);";
        Map<String, Object> map = new HashMap();
        map.put("city", entity.getCity());
        map.put("timestamp", entity.getTimestamp());
        map.put("positive", entity.getPositive());
        map.put("negative", entity.getNegative());
        map.put("total", entity.getTotal());
        map.put("w_positive", entity.getW_positive());
        map.put("w_negative", entity.getW_negative());
        map.put("w_total", entity.getW_total());

        return namedParameterJdbcTemplate.update(sql, map);
    }

    public List<TweetStatisticEntity> queryAqi(String city, int limit) {
        String sql = "select * from t_aqi_data t where t.city=:city order by t.timestamp desc limit :limit;";
        Map<String, Object> map = new HashMap();
        map.put("city", city);
        map.put("limit", limit);

        return namedParameterJdbcTemplate.query(sql, map, new TweetStatisticEntity());
    }
}