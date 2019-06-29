package hk.hku.cloud.ml;

import hk.hku.cloud.kafka.domain.TweetStatisticEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final String model = "CloudWeb/src/main/resources/model/RandomForest.model";
    private static RandomTree INSTANCE;
    private static Classifier classifier8;
    private static ArrayList<Attribute> attributes = new ArrayList<>();

    public RandomTree() {
        try {
            classifier8 = (Classifier) weka.core.SerializationHelper.read(model);
        } catch (Exception e) {
            logger.error("Classifier Exception", e);
        }

        attributes.add(new Attribute("positive"));
        attributes.add(new Attribute("negative"));
        attributes.add(new Attribute("totalCount"));
        attributes.add(new Attribute("weatherPositive"));
        attributes.add(new Attribute("weatherNegative"));
        attributes.add(new Attribute("weatherCount"));
    }

    public static RandomTree getInstance() {
        synchronized (RandomTree.class) {
            if (INSTANCE == null) {
                INSTANCE = new RandomTree();
            }
            return INSTANCE;
        }
    }

    public double predictAQI(TweetStatisticEntity entity) {
        Instance instance = new DenseInstance(attributes.size());

        instance.setValue(0, entity.getPositive());
        instance.setValue(1, entity.getPositive());
        instance.setValue(2, entity.getTotal());
        instance.setValue(3, entity.getW_positive());
        instance.setValue(4, entity.getW_negative());
        instance.setValue(5, entity.getW_total());

        Instances instances = new Instances("repo_popular", attributes, 0);
        instances.setClassIndex(instances.numAttributes() - 1);
        instances.add(instance);

        logger.info(instances.instance(0).toString());

        double aqi_value = 0;
        try {
            aqi_value = classifier8.classifyInstance(instances.instance(0));
        } catch (Exception e) {
            logger.error("predict exception", e);
        }

        return aqi_value;
    }
}