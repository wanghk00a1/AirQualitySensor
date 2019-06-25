package hk.hku.flink.utils;

/**
 * @author: LexKaing
 * @create: 2019-06-25 14:50
 * @description:
 **/
public class GeoCity {

    final static double[] SF_AREA = {-123.1512, 37.0771, -121.3165, 38.5396};
    final static double[] NY_AREA = {-74.255735, 40.496044, -73.700272, 40.915256};
    final static double[] LA_AREA = {-118.6682, 33.7037, -118.1553, 34.3373};
    final static double[] CHICAGO = {-87.940267, 41.644335, -87.524044, 42.023131};
    final static double[] LONDON = {-0.5104, 51.2868, 0.334, 51.6919};

    public static String geoToCity(double longitude, double latitude) {

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