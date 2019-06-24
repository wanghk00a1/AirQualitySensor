package hk.hku.flink.process;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: LexKaing
 * @create: 2019-06-24 15:13
 * @description:
 **/
public class PreProcessFile {

    static double[] SF_AREA = {-123.1512, 37.0771, -121.3165, 38.5396};
    static double[] NY_AREA = {-74.255735, 40.496044, -73.700272, 40.915256};
    static double[] LA_AREA = {-118.6682, 33.7037, -118.1553, 34.3373};
    static double[] CHICAGO = {-87.940267, 41.644335, -87.524044, 42.023131};
    static double[] LONDON = {-0.5104, 51.2868, 0.334, 51.6919};


    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


    /*
    java -classpath StreamProcessorFlink-jar-with-dependencies.jar hk.hku.flink.process.PreProcessFile \
    /home/hduser/data-bak0621/twitter.log /home/hduser/data-bak0621/preprocess-twitter.log 5
     */
    public static void main(String[] args) {
//        String inputFile = args[0];
//        String outputFile = args[1];
//        int memory = Integer.valueOf(args[2]);

        PreProcessFile preProcessFile = new PreProcessFile();
//        preProcessFile.largeFileIO(inputFile, outputFile);
        preProcessFile.largeFileIO("logs/twitter_london.data",
                "/Users/Kai/Downloads/twitter/preprocess_twitter_london.log",
                5);

    }

    public void largeFileIO(String inputFile, String outputFile, int memory) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(inputFile)));
//
            BufferedReader in = new BufferedReader(new InputStreamReader(bis, "utf-8"), memory * 1024 * 1024);
//            FileWriter fw = new FileWriter(outputFile);

            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            System.out.println(sdf.format(new Date()));

            String line;
            while ((line = in.readLine()) != null) {

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
//                fw.append(String.valueOf(status.getId())).append(",")
//                        .append(String.valueOf(status.getCreatedAt().getTime())).append(",")
//                        .append(city).append(",")
//                        .append(String.valueOf(sentiment)).append(",")
//                        .append(text)
//                        .append("\n");

                writer.write(status.getId() + "," + status.getCreatedAt().getTime() + ","
                        + city + "," + sentiment + "," + text + "\n");
            }
            in.close();
//            fw.flush();
//            fw.close();

            writer.close();
            System.out.println(sdf.format(new Date()));

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