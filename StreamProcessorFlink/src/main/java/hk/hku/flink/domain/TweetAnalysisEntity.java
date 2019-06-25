package hk.hku.flink.domain;

import static hk.hku.flink.utils.Constants.DELIMITER;

/**
 * @author: LexKaing
 * @create: 2019-06-20 16:00
 * @description: id¦text¦username¦geo¦language¦weather
 **/
public class TweetAnalysisEntity {
    private long id;
    private String text;
    private String username;
    private String geo;
    private String language;
    private Boolean weather;
    private int sentiment;

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

    public Boolean getWeather() {
        return weather;
    }

    public void setWeather(Boolean weather) {
        this.weather = weather;
    }

    @Override
    public String toString() {
        return this.id + DELIMITER
                + this.geo + DELIMITER
                + this.language + DELIMITER
                + this.weather + DELIMITER
                + this.sentiment + DELIMITER
                + this.text;
    }
}