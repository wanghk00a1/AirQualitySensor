package hk.hku.spark.process;

import com.google.gson.Gson;
import twitter4j.JSONArray;
import twitter4j.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by wanghanke on 24/6/2019.
 */


/*
    时间 —>   时间戳
    城市 -> 	纽约 NY
		    洛杉矶 LA
		    芝加哥 CHICAGO
		    拉斯维加斯 LV
		    伦敦 LONDON
 */
public class AqiDataPrepross {
    public static void main(String[] args) {
        File file = new File("../AQIFetcher-out.log");
        File resultFile = new File("../AQI.csv");

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));

            String tempString = null;

            int line = 1;
            while ((tempString = reader.readLine()) != null) {
//                System.out.println("line " + line + ": " + tempString);
                JSONObject jsonObject = new JSONObject(tempString);
                String cityId = jsonObject.getString("id");
                String cityName = null;
                switch(cityId) {
                    case "gXTDkEBCX9BBKe5wc": {cityName = "NY";break;}
                    case "f8LwYNnj49MDEnSPx": {cityName = "LA";break;}
                    case "5uZXFy8X6cXdTTPtJ": {cityName = "CHICAGO";break;}
                    case "6o5AkzkwfPkDCKt8X": {cityName = "LV";break;}
                    case "7McFS9nFSf5TQmwva": {cityName = "LONDON";break;}
                }

                JSONObject measurements = jsonObject.getJSONObject("measurements");
                JSONArray hourlyAqi = measurements.getJSONArray("hourly");

                for(int i=0;i<hourlyAqi.length();i++){
                    String timeStemp = hourlyAqi.getJSONObject(i).getString("ts");
                    String response = timeStemp.replaceAll("T"," ");
                    String response1 = response.replaceAll(".000Z","");
                    String format = "yyyy-MM-dd HH:mm:ss";
                    SimpleDateFormat sdf = new SimpleDateFormat(format);
                    String time = String.valueOf(sdf.parse(response1).getTime()/1000);

                    Integer AQI = hourlyAqi.getJSONObject(i).getInt("aqi");

                    writer.newLine();
                    writer.write(cityName + "," + time + "," + AQI);
                }
                line++;
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}