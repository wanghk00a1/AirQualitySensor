package hk.hku.spark.process;

import org.apache.spark.sql.sources.In;
import scala.Int;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by wanghanke on 25/6/2019.
 */
public class TimelyAQICount {
    public static void main(String[] args) {
        File file = new File("data/tweetAQI-0625-media.csv");
        File resultFile = new File("data/timelyAqiCount-0625-media.csv");
        File resultFile1 = new File("data/timelyAqiCount-0625-firstOrderDifferent.csv");

        List<String> time = new ArrayList<>();
        List<Integer> positive = new ArrayList<>();
        List<Integer> negative = new ArrayList<>();
        List<Integer> totalCount = new ArrayList<>();
        List<Integer> weatherPositive = new ArrayList<>();
        List<Integer> weatherNegative = new ArrayList<>();
        List<Integer> weatherCount = new ArrayList<>();
        List<Integer> aqi = new ArrayList<>();

        try {
            BufferedReader tweetAqiData = new BufferedReader(new FileReader(file));
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(resultFile1, true));

            tweetAqiData.readLine();
            String line = null;


            while ((line = tweetAqiData.readLine()) != null) {
                String item[] = line.split(",");

                if(!String.valueOf(item[3]).equals("LONDON") ||
                        !String.valueOf(item[5]).equals("en") ||
                        !String.valueOf(item[6]).equals("false")){
                    continue;
                }

                int aqiNum = Integer.valueOf(item[0]);
                int sentiment = Integer.valueOf(item[4]);

                Long timeStamp = Long.valueOf(item[2]);
                String timeString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(timeStamp));

                int i=0;
                boolean found = false;
                for(i=0;i<time.size();i++){
                    if(time.get(i).equals(timeString)){
                        found = true;
                        if(weatherRelated(item)) {
                            if (sentiment == 1){
                                positive.set(i, positive.get(i) + 1); weatherPositive.set(i, weatherPositive.get(i)+1);
                            }
                            else if (sentiment == -1){
                                negative.set(i, negative.get(i) + 1); weatherNegative.set(i, weatherNegative.get(i)+1);
                            }

                            totalCount.set(i, totalCount.get(i) + 1); weatherCount.set(i, weatherCount.get(i)+1);
                        }
                        else{
                            if (sentiment == 1){
                                positive.set(i, positive.get(i) + 1);
                            }
                            else if (sentiment == -1){
                                negative.set(i, negative.get(i) + 1);
                            }

                            totalCount.set(i, totalCount.get(i) + 1);
                        }
                    }
                }
                if(!found){
                    time.add(timeString);
                    if(weatherRelated(item)) {
                        if (sentiment == 1){
                            positive.add(1); negative.add(0);weatherPositive.add(1);weatherNegative.add(0);
                        }
                        else if (sentiment == -1){
                            positive.add(0);negative.add(1); weatherPositive.add(0);weatherNegative.add(1);
                        }
                        else{
                            positive.add(0);negative.add(0); weatherPositive.add(0);weatherNegative.add(1);
                        }

                        totalCount.add(1); weatherCount.add(1);
                    }
                    else{
                        if (sentiment == 1){
                            positive.add(1);negative.add(0); weatherPositive.add(0);weatherNegative.add(0);
                        }
                        else if (sentiment == -1){
                            positive.add(0);negative.add(1); weatherPositive.add(0);weatherNegative.add(0);
                        }
                        else {
                            positive.add(0);negative.add(0); weatherPositive.add(0);weatherNegative.add(0);
                        }
                        totalCount.add(1); weatherCount.add(0);
                    }
                    aqi.add(aqiNum);
                }
            }

            for(int i=0;i<time.size();i++){
                writer.newLine();
                writer.write(Integer.valueOf(time.get(i).substring(11, 13)) + "," + positive.get(i) + "," + negative.get(i) + "," + totalCount.get(i) + "," +
                        weatherPositive.get(i) + "," + weatherNegative.get(i) + "," + weatherCount.get(i) + "," + aqi.get(i));
                writer.flush();
            }
            writer.close();

//            for(int i=1;i<time.size();i++){
//                writer1.newLine();
//                writer1.write( (positive.get(i)-positive.get(i-1)) + "," +
//                        (negative.get(i) - negative.get(i-1)) + "," +
//                        (totalCount.get(i) -totalCount.get(i-1)) + "," +
//                        (weatherPositive.get(i) - weatherPositive.get(i-1)) + "," +
//                        (weatherNegative.get(i) - weatherNegative.get(i-1)) + "," +
//                        (weatherCount.get(i) - weatherCount.get(i-1)) + "," +
//                        (aqi.get(i) - aqi.get(i-1)));
//                writer1.flush();
//            }
//            writer1.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static boolean weatherRelated(String[] texts){
        Long num = Long.valueOf(0);
        for(String text: texts) {
            num += Arrays.stream(new String[]{"weather", "aqi", "health",
                    "smoke", "air", "pollution", "breathe", "lungs",
                    "smog", "haze", "cough"}).filter(word -> text.contains(word)).count();

        }
        if(num>0) return true;
        else return false;
    }
}
