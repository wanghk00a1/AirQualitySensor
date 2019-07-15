package hk.hku.cloud.ml;

import hk.hku.cloud.kafka.domain.TweetStatisticEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.util.ArrayList;

/**
 * @author: LexKaing
 * @create: 2019-06-28 23:08
 * @description:
 **/
public class RandomTree {

    private static final Logger logger = LoggerFactory.getLogger(RandomTree.class);
//    private static final String model = "model/RandomForest.model";
    private static RandomTree INSTANCE;
    private static Classifier classifier8;
    private static ArrayList<Attribute> attributes = new ArrayList<>();

    public RandomTree(String modelPath) {
        try {
            Resource resource = new ClassPathResource(modelPath);
            classifier8 = (Classifier) weka.core.SerializationHelper.read(resource.getInputStream());
            logger.info("random tree init");
        } catch (Exception e) {
            logger.error("Classifier Exception", e);
        }

        attributes.add(new Attribute("positive"));
        attributes.add(new Attribute("negative"));
        attributes.add(new Attribute("neutral"));
        attributes.add(new Attribute("weatherPositive"));
        attributes.add(new Attribute("weatherNegative"));
        attributes.add(new Attribute("weatherNeutral"));
    }

    public static RandomTree getInstance(String modelPath) {
        synchronized (RandomTree.class) {
            if (INSTANCE == null) {
                INSTANCE = new RandomTree(modelPath);
            }
            return INSTANCE;
        }
    }

    public double predictAQI(int positive,int negative,int neutral,int w_positive,int w_negative,int w_neutral) {
        Instance instance = new DenseInstance(attributes.size());

        instance.setValue(0,positive);
        instance.setValue(1, negative);
        instance.setValue(2, neutral);
        instance.setValue(3, w_positive);
        instance.setValue(4, w_negative);
        instance.setValue(5, w_neutral);

        Instances instances = new Instances("repo_popular", attributes, 0);
        instances.setClassIndex(instances.numAttributes() - 1);
        instances.add(instance);

//        logger.info(instances.instance(0).toString());

        double aqi_value = 0;
        try {
            aqi_value = classifier8.classifyInstance(instances.instance(0));
        } catch (Exception e) {
            logger.error("predict exception", e);
        }

        return aqi_value;
    }
}