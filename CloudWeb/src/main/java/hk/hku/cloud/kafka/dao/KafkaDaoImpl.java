package hk.hku.cloud.kafka.dao;

import hk.hku.cloud.kafka.domain.AqiEntity;
import hk.hku.cloud.kafka.domain.TweetStatisticEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public int insertPredictAqi(TweetStatisticEntity entity) {
        String sql = "insert into t_tweet_aqi_data(city,timestamp,positive,negative,total,w_positive,w_negative,w_total,random_tree) "
                + "values(:city,:timestamp,:positive,:negative,:total,:w_positive,:w_negative,:w_total,:random_tree);";
        Map<String, Object> map = new HashMap();
        map.put("city", entity.getCity());
        map.put("timestamp", entity.getTimestamp());
        map.put("positive", entity.getPositive());
        map.put("negative", entity.getNegative());
        map.put("total", entity.getTotal());
        map.put("w_positive", entity.getW_positive());
        map.put("w_negative", entity.getW_negative());
        map.put("w_total", entity.getW_total());
        map.put("random_tree", entity.getRandom_tree());

        return namedParameterJdbcTemplate.update(sql, map);
    }

    public int[] insertPredictAqiList(List<TweetStatisticEntity> entityList) {
        String sql = "insert into t_tweet_aqi_data(city,timestamp,positive,negative,total,w_positive,w_negative,w_total,random_tree) "
                + "values(:city,:timestamp,:positive,:negative,:total,:w_positive,:w_negative,:w_total,:random_tree);";

        Map<String, Object>[] batchValues = new Map[entityList.size()];
        int i = 0;
        for (TweetStatisticEntity entity : entityList) {
            Map<String, Object> map = new HashMap();
            map.put("city", entity.getCity());
            map.put("timestamp", entity.getTimestamp());
            map.put("positive", entity.getPositive());
            map.put("negative", entity.getNegative());
            map.put("total", entity.getTotal());
            map.put("w_positive", entity.getW_positive());
            map.put("w_negative", entity.getW_negative());
            map.put("w_total", entity.getW_total());
            map.put("random_tree", entity.getRandom_tree());

            batchValues[i] = map;
            i++;
        }

        int[] result = namedParameterJdbcTemplate.batchUpdate(sql, batchValues);
        return result;
    }

    public List<TweetStatisticEntity> queryPastOneHourData(String city, String timestamp, String pastTimestamp) {
        String sql = "select * from t_tweet_aqi_data t " +
                "where t.city=city and t.timestamp<:timestamp and t.timestamp>=:pastTimestamp;";
        Map<String, Object> map = new HashMap();
        map.put("city", city);
        map.put("timestamp", timestamp);
        map.put("pastTimestamp", pastTimestamp);

        return namedParameterJdbcTemplate.query(sql, map, new TweetStatisticEntity());
    }

    public List<TweetStatisticEntity> queryPredictAqi(String city, int limit) {
        String sql = "select * from t_tweet_aqi_data t where t.city=:city order by t.timestamp desc limit :limit;";
        Map<String, Object> map = new HashMap();
        map.put("city", city);
        map.put("limit", limit);

        return namedParameterJdbcTemplate.query(sql, map, new TweetStatisticEntity());
    }

    public int[] insertActualAQI(List<AqiEntity> aqiEntities) {
        String sql = "insert into t_aqi_data(city,timestamp,aqi) "
                + "values(:city,:timestamp,:aqi) "
                + "on duplicate key update aqi=:aqi;";
        Map<String, Object>[] batchValues = new Map[aqiEntities.size()];
        int i = 0;
        for (AqiEntity tmp : aqiEntities) {
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("city", tmp.getCity());
            paramMap.put("timestamp", tmp.getTimestamp());
            paramMap.put("aqi", tmp.getAqi());
            batchValues[i] = paramMap;
            i++;
        }

        int[] result = namedParameterJdbcTemplate.batchUpdate(sql, batchValues);
        return result;
    }

    public List<AqiEntity> queryActualAqi(String city, int limit) {
        String sql = "select * from t_aqi_data t where t.city=:city order by t.timestamp desc limit :limit;";
        Map<String, Object> map = new HashMap();
        map.put("city", city);
        map.put("limit", limit);

        return namedParameterJdbcTemplate.query(sql, map, new AqiEntity());
    }
}