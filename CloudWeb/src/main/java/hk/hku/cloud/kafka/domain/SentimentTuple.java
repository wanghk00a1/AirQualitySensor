package hk.hku.cloud.kafka.domain;

/**
 * @author: LexKaing
 * @create: 2019-04-01 23:19
 * @description: 从Kafka中读取的 Spark 实时处理完的数据
 * Tuple format: Tweet ID, Screen Name, Tweet Text, Core NLP Polarity, MLlib Polarity,
 * Latitude, Longitude,Profile Image URL, Tweet Date.
 * <p>
 * Long, String, String, Int, Int, Double, Double, String, String
 * <p>
 * Polarity: 1:positive; 0:neutral; -1:negative
 * Latitude/Longitude: -1.0: 地理信息缺失
 * Date 格式: new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")
 **/
public class SentimentTuple {
    private String id;
    private String name;
    private String text;
    private int nlpPolarity;
    private int nbPolarity;
    private int dlPolarity;
    private double latitude;
    private double longitude;
    private String image;
    private String date;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getNlpPolarity() {
        return nlpPolarity;
    }

    public void setNlpPolarity(int nlpPolarity) {
        this.nlpPolarity = nlpPolarity;
    }

    public int getNbPolarity() {
        return nbPolarity;
    }

    public void setNbPolarity(int nbPolarity) {
        this.nbPolarity = nbPolarity;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getDlPolarity() {
        return dlPolarity;
    }

    public void setDlPolarity(int dlPolarity) {
        this.dlPolarity = dlPolarity;
    }
}