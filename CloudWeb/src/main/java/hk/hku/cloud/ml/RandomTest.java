package hk.hku.cloud.ml;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @author: LexKaing
 * @create: 2019-06-30 14:49
 * @description:
 **/
public class RandomTest {

    public static void main(String[] args) {

        DecimalFormat df = new DecimalFormat("#.00");

        try {
            String model = "model/RandomTree7981-14.model";
            Resource resource = new ClassPathResource("model/t_tweet_aqi_data.csv");

            FileReader fr = new FileReader(resource.getFile());
            BufferedReader bufferedReader = new BufferedReader(fr);

            BufferedReader actualAqi = new BufferedReader(new FileReader(new ClassPathResource("model/t_aqi_data.csv").getFile()));
            String line = null;
            List<String> aqiList = new ArrayList<>();
            while ((line = actualAqi.readLine()) != null) {

                String[] tmp = line.split(",");
                if (tmp[0].equals("LONDON"))
                    aqiList.add(tmp[1] + "," + tmp[2]);
            }

            System.out.println(aqiList.get(2));

            List<Integer> positive = new ArrayList<>();
            List<Integer> negative = new ArrayList<>();
            List<Integer> total = new ArrayList<>();
            List<Integer> w_positive = new ArrayList<>();
            List<Integer> w_negative = new ArrayList<>();
            List<Integer> w_total = new ArrayList<>();

            while ((line = bufferedReader.readLine()) != null) {
                //LONDON,2019-06-29 17:20:00,00016,00086,00146,00000,00001,00001,39.27
                String[] tmp = line.split(",");
                positive.add(Integer.valueOf(tmp[2]));
                negative.add(Integer.valueOf(tmp[3]));
                total.add(Integer.valueOf(tmp[4]));
                w_positive.add(Integer.valueOf(tmp[5]));
                w_negative.add(Integer.valueOf(tmp[6]));
                w_total.add(Integer.valueOf(tmp[7]));

                if (positive.size() > 6) {
                    positive.remove(0);
                    negative.remove(0);
                    total.remove(0);
                    w_positive.remove(0);
                    w_negative.remove(0);
                    w_total.remove(0);
                }

                int sumP = positive.stream().mapToInt(x -> x).sum();
                int sumN = negative.stream().mapToInt(x -> x).sum();
                int sumT = total.stream().mapToInt(x -> x).sum();
                int sumWP = w_positive.stream().mapToInt(x -> x).sum();
                int sumWN = w_negative.stream().mapToInt(x -> x).sum();
                int sumWT = w_total.stream().mapToInt(x -> x).sum();


                String actualAqiData = aqiList.stream().filter(x -> x.startsWith(tmp[1].substring(0, 13))).findFirst().get();

                String str = sumP + "," + sumN + "," + sumT + "," + sumWP + "," + sumWN + "," + sumWT;
                double predict = RandomTree.getInstance(model).predictAQI(sumP, sumN, sumT, sumWP, sumWN, sumWT);
                System.out.println(line + " || " + actualAqiData + " || " + str + "," + df.format(predict));

                if (positive.size() > 6) {
                    System.out.println("error : " + positive.size());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}