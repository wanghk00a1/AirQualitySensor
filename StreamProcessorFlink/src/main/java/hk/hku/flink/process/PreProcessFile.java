package hk.hku.flink.process;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.io.*;

/**
 * @author: LexKaing
 * @create: 2019-06-24 15:13
 * @description:
 **/
public class PreProcessFile {

    private static final Logger logger = LoggerFactory.getLogger(PreProcessFile.class);

    static double[] SF_AREA = {-123.1512, 37.0771, -121.3165, 38.5396};
    static double[] NY_AREA = {-74.255735, 40.496044, -73.700272, 40.915256};
    static double[] LA_AREA = {-118.6682, 33.7037, -118.1553, 34.3373};
    static double[] CHICAGO = {-87.940267, 41.644335, -87.524044, 42.023131};
    static double[] LONDON = {-0.5104, 51.2868, 0.334, 51.6919};

    public static void main(String[] args) {

        logger.info("start");

        String inputFile = args[0];
        String outputFile = args[1];

        logger.info(inputFile + "," + outputFile);

        PreProcessFile preProcessFile = new PreProcessFile();
        preProcessFile.largeFileIO(inputFile, outputFile);

    }

    public void largeFileIO(String inputFile, String outputFile) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(inputFile)));
            BufferedReader in = new BufferedReader(new InputStreamReader(bis, "utf-8"), 10 * 1024 * 1024);// 10M缓存
            FileWriter fw = new FileWriter(outputFile);


            while (in.ready()) {
                String line = in.readLine();
                Status status = TwitterObjectFactory.createStatus(line);
                String text = status.getText().replaceAll("\n", "");

                int sentiment = CoreNLPSentimentAnalyzer.getInstance().computeWeightedSentiment(text);

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

                // id,date,city,sentiment,text
                fw.append(String.valueOf(status.getId())).append(",")
                        .append(String.valueOf(status.getCreatedAt().getTime())).append(",")
                        .append(city).append(",")
                        .append(String.valueOf(sentiment)).append(",")
                        .append(text)
                        .append("\n");
            }
            in.close();
            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String returnCity(double longitude, double latitude) {
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