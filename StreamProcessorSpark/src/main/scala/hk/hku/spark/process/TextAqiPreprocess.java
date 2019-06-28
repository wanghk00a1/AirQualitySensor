package hk.hku.spark.process;

import java.io.*;
import java.text.ParseException;

/**
 * Created by wanghanke on 28/6/2019.
 */
public class TextAqiPreprocess {
    public static void main(String[] args) {
        File file = new File("../part-00000.csv");
        File resultFile = new File("../temp.csv");

        try{
            BufferedReader reader = new BufferedReader(new FileReader(file));
            BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));

            String tempString = null;

            int line = 1;
            while ((tempString = reader.readLine()) != null) {

            }

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
