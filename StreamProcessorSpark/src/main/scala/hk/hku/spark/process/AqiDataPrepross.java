package hk.hku.spark.process;

import com.google.gson.Gson;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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
        File file = new File("data/AQIFetcher-out-0628.log");
        File resultFile = new File("data/AQI.csv");
        File finalAqiFile = new File("data/finalAQI.csv");

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));

            String tempString = null;

            while ((tempString = reader.readLine()) != null) {
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(tempString);
                } catch (JSONException e) {
                    continue;
                }
                String cityId = jsonObject.getString("id");
                String cityName = null;
                switch (cityId) {
                    case "gXTDkEBCX9BBKe5wc": {
                        cityName = "NY";
                        break;
                    }
                    case "f8LwYNnj49MDEnSPx": {
                        cityName = "LA";
                        break;
                    }
                    case "5uZXFy8X6cXdTTPtJ": {
                        cityName = "CHICAGO";
                        break;
                    }
                    case "6o5AkzkwfPkDCKt8X": {
                        cityName = "LV";
                        break;
                    }
                    case "7McFS9nFSf5TQmwva": {
                        cityName = "LONDON";
                        break;
                    }
                }

                JSONObject measurements = jsonObject.getJSONObject("measurements");
                JSONArray hourlyAqi = measurements.getJSONArray("hourly");

                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");

                for (int i = 0; i < hourlyAqi.length(); i++) {

                    String timeStamp = hourlyAqi.getJSONObject(i).getString("ts");

                    Date date = df.parse(timeStamp);
                    String time = sdf.format(date);

                    Integer AQI = hourlyAqi.getJSONObject(i).getInt("aqi");

                    if (timeStamp.startsWith("2019-06-28T07") && cityName.equals("LONDON")) {
                        System.out.println(timeStamp + ", date : " + date + "," + AQI);
                        break;
                    }

//                    writer.newLine();
//                    writer.write(cityName + "," + time + "," + AQI);
                }
            }
//            writer.close();

//            BufferedReader reader1 = new BufferedReader(new FileReader(resultFile));
//            BufferedWriter writer1 = new BufferedWriter(new FileWriter(finalAqiFile, true));
//            List<String> list = new ArrayList<>();
//
//            tempString = null;

//            while ((tempString = reader1.readLine()) != null) {
//
//                boolean found = false;
//                for(int i=0;i<list.size();i++){
//                    if(tempString.equals(list.get(i))){
//                        found = true;
//                        break;
//                    }
//                }
//                if(!found) {
//                    list.add(tempString);
//                    writer1.newLine();
//                    writer1.write(tempString);
//                }
//            }
//            writer1.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
