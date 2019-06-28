package hk.hku.cloud.kafka.domain;


/**
 * @author: LexKaing
 * @create: 2019-06-20 16:00
 * @description: 解析后的 tweet 数据，只有coreNlp
 **/
public class TweetAnalysisEntity {
    private long id;
    private String text;
    private String username;
    private String geo;
    private String language;
    private long createtime;
    private Boolean hasWeather;
    private int sentiment;
    private Boolean hasMedia;
    private Boolean isRetweet;

    public long getCreatetime() {
        return createtime;
    }

    public void setCreatetime(long createtime) {
        this.createtime = createtime;
    }

    public Boolean getHasMedia() {
        return hasMedia;
    }

    public void setHasMedia(Boolean hasMedia) {
        this.hasMedia = hasMedia;
    }

    public Boolean getRetweet() {
        return isRetweet;
    }

    public void setRetweet(Boolean retweet) {
        isRetweet = retweet;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public Boolean getHasWeather() {
        return hasWeather;
    }

    public void setHasWeather(Boolean hasWeather) {
        this.hasWeather = hasWeather;
    }

    @Override
    public String toString() {
        return this.id + ","
                + this.geo + ","
                + this.language + ","
                + this.hasWeather + ","
                + this.sentiment + ","
                + this.text;
    }
}