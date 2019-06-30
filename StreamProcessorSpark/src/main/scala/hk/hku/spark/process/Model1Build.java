package hk.hku.spark.process;

import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.*;
import weka.core.converters.ArffLoader;
import weka.core.converters.CSVLoader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;


/**
 * Created by wanghanke on 28/6/2019.
 */
public class Model1Build {
    static CSVLoader csv = new CSVLoader();

    public static void main(String[] args) throws Exception {
//        Classifier m_classifier = new RandomForest();
//        File inputFile = new File("data/timelyAqiCount-0628-imbalance.csv");//训练语料文件

//        csv.setFile(inputFile);
//        Instances instancesTrain = csv.getDataSet(); // 读入训练文件
//
        File inputFile = new File("data/timelyAqiCount-0628-imbalance.csv");//测试语料文件
        csv.setFile(inputFile);
        Instances instancesTest = csv.getDataSet(); // 读入测试文件
        instancesTest.setClassIndex(7); //设置分类属性所在行号（第一行为0号），
//
//        // instancesTest.numAttributes()可以取得属性总数
        double sum = instancesTest.numInstances(),//测试语料实例数
                right = 0.0f;

//        instancesTrain.setClassIndex(0);

//        m_classifier.buildClassifier(instancesTrain); //训练
//        System.out.println(m_classifier);

        // 保存模型
//        SerializationHelper.write("model/RandomForest.model", m_classifier);//参数一为模型保存文件，classifier4为要保存的模型

//        for(int  i = 0;i<sum;i++)//测试分类结果  1
//        {
//            System.out.println(m_classifier.classifyInstance(instancesTest.instance(i)));
//            right = right + Math.abs(m_classifier.classifyInstance(instancesTest.instance(i))-instancesTest.instance(i).classValue());
//
//        }
//        System.out.println(right+" "+right/sum);

        // 获取上面保存的模型
        Classifier classifier8 = (Classifier) weka.core.SerializationHelper.read("model/RandomTree7981-14.model");
        double right2 = 0.0f;
        instancesTest.deleteAttributeAt(0);
        for(int  i = 0;i<sum;i++)//测试分类结果  2 (通过)
        {
//            System.out.println(instancesTest.instance(i));
            System.out.println(classifier8.classifyInstance(instancesTest.instance(i))+"  "+instancesTest.instance(i).classValue());
            right2 = right2 + Math.abs(classifier8.classifyInstance(instancesTest.instance(i))-instancesTest.instance(i).classValue());
        }
        System.out.println(right2/sum);

//        System.out.println(adi_predict(210,1255,2127,3,45,56));

    }

    private static double adi_predict(double a, double b, double c, double d, double e, double f) throws Exception {
        Classifier classifier8 = (Classifier) weka.core.SerializationHelper.read("model/RandomForest.model");

        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("positive"));
        attributes.add(new Attribute("negative"));
        attributes.add(new Attribute("totalCount"));
        attributes.add(new Attribute("weatherPositive"));
        attributes.add(new Attribute("weatherNegative"));
        attributes.add(new Attribute("weatherCount"));

        Instance instance = new DenseInstance(attributes.size());

        instance.setValue(0,a);
        instance.setValue(1,b);
        instance.setValue(2,c);
        instance.setValue(3,d);
        instance.setValue(4,e);
        instance.setValue(5,f);

        Instances instances = new Instances("repo_popular",attributes,0);
        instances.setClassIndex(instances.numAttributes() - 1);
        instances.add(instance);

        System.out.println(instances.instance(0).toString());

        double aqi_value = classifier8.classifyInstance(instances.instance(0));

        return aqi_value;
    }
}
