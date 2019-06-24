package hk.hku.flink.process;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Status;

import static hk.hku.flink.utils.Constants.COMMA;

/**
 * @author: LexKaing
 * @create: 2019-06-24 20:16
 * @description:
 **/
public class TweetPraseMap implements MapFunction<Status, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String map(Status status) throws Exception {
        String text = status.getText().replaceAll("\n", "");

//        int sentiment = CoreNLPSentimentAnalyzer.getInstance().computeWeightedSentiment(text);
        int sentiment = 0;

        String city = "NULL";
        if (status.getGeoLocation() != null) {
            city = returnCity(status.getGeoLocation().getLongitude(),
                    status.getGeoLocation().getLatitude());

        } else if (status.getPlace() != null) {
            double longitude = 0.0, latitude = 0.0;
            for (GeoLocation[] coorList : status.getPlace().getBoundingBoxCoordinates()) {

                for (GeoLocation coor : coorList) {
                    longitude += coor.getLongitude();
                    latitude += coor.getLatitude();
                }
            }
            city = returnCity(longitude / 4, latitude / 4);
        }

        return status.getId() + "," + status.getCreatedAt().getTime() + "," + city + "," + sentiment + "," + text;
    }

    private String returnCity(double longitude, double latitude) {
        double[] SF_AREA = {-123.1512, 37.0771, -121.3165, 38.5396};
        double[] NY_AREA = {-74.255735, 40.496044, -73.700272, 40.915256};
        double[] LA_AREA = {-118.6682, 33.7037, -118.1553, 34.3373};
        double[] CHICAGO = {-87.940267, 41.644335, -87.524044, 42.023131};
        double[] LONDON = {-0.5104, 51.2868, 0.334, 51.6919};
        if (SF_AREA[0] <= longitude && longitude <= SF_AREA[2] && SF_AREA[1] <= latitude && latitude <= SF_AREA[3])
            return "SF";
        else if (NY_AREA[0] <= longitude && longitude <= NY_AREA[2] && NY_AREA[1] <= latitude && latitude <= NY_AREA[3])
            return "NY";
        else if (LA_AREA[0] <= longitude && longitude <= LA_AREA[2] && LA_AREA[1] <= latitude && latitude <= LA_AREA[3])
            return "LA";
        else if (CHICAGO[0] <= longitude && longitude <= CHICAGO[2] && CHICAGO[1] <= latitude && latitude <= CHICAGO[3])
            return "CHICAGO";
        else if (LONDON[0] <= longitude && longitude <= LONDON[2] && LONDON[1] <= latitude && latitude <= LONDON[3])
            return "LONDON";
        else
            return "NULL";
    }
}