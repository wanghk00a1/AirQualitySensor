package hk.hku.spark.process;

import org.apache.commons.net.ntp.TimeStamp;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wanghanke on 24/6/2019.
 */
public class TweetAqiSentimentPreprocess {

    public static void main(String[] args) {
        String path = "data/tweet-0630/twitter_london_ny_preprocess";
        File file = new File(path);
        func(file);

    }

    private static void func(File file){
        File[] fs = file.listFiles();
        for(File f:fs){
            if(f.isDirectory())
                func(f);
            if(f.isFile())
                process(f.getAbsolutePath());
        }
    }

    public static void process(String TweetFilePass) {
        File AqiFile = new File("data/finalAQI.csv");
        File TweetFile = new File(TweetFilePass);
        System.out.println(TweetFilePass);
        File TweetAqiFile = new File("data/tweetAQI-0630-media.csv");
        try {
            BufferedReader AqiData = new BufferedReader(new FileReader(AqiFile));
            BufferedReader TweetData = new BufferedReader(new FileReader(TweetFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(TweetAqiFile, true));

            AqiData.readLine();
            String line = null;
            List<String> AqiCity = new ArrayList<>();
            List<String> AdiTimeStamp = new ArrayList<>();
            List<Integer> AqiValue = new ArrayList<>();
            while ((line = AqiData.readLine()) != null) {
                String item[] = line.split(",");

                AqiCity.add(item[0]);

                Long timeStamp = Long.valueOf(item[1]);
                String timeString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(timeStamp*1000));
                AdiTimeStamp.add(timeString);

                AqiValue.add(Integer.valueOf(item[2]));

            }

            TweetData.readLine();
            line = null;
            while ((line = TweetData.readLine()) != null) {
                try {
                    String item[] = line.split(",");

                    Long timeStamp = Long.valueOf(item[1]);
                    String timeString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(timeStamp));

                    String city = item[2];

                    for (int i = 0; i < AqiCity.size(); i++) {
                        Integer aqi = null;
                        if (timeString.equals(AdiTimeStamp.get(i)) && city.equals(AqiCity.get(i))) {
                            aqi = AqiValue.get(i);
                            System.out.println(i);
                            writer.newLine();
                            writer.write(aqi + "," + line);
                            writer.flush();
                            break;
                        }
                    }
                } catch (Exception e){
                    e.printStackTrace();
                    continue;
                }
            }


        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
