package hk.hku.spark.process;

import java.io.*;

/**
 * Created by wanghanke on 12/7/2019.
 */
public class TimelyAqiCountBalance {
    public static void main(String[] args) {
        File File = new File("data/timelyAqiCount-0710-all.csv");
        File resultFile = new File("data/timelyAqiCount-0710-all-balance.csv");

        try {
            BufferedReader tweetAqiData = new BufferedReader(new FileReader(File));
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));



            tweetAqiData.readLine();
            String line = null;


            while ((line = tweetAqiData.readLine()) != null) {
                writer.newLine();
                writer.write(line);
                writer.flush();

                String item[] = line.split(",");

                if(Integer.valueOf(item[item.length-1])>34 && Integer.valueOf(item[item.length-1])<84){
                    writer.newLine();
                    writer.write(line);
                    writer.flush();
                }

                if(Integer.valueOf(item[item.length-1])>84){
                    for(int i=0;i<18;i++){
                        writer.newLine();
                        writer.write(line);
                        writer.flush();
                    }
                }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
