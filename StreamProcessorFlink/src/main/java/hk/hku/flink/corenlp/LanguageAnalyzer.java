package hk.hku.flink.corenlp;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import hk.hku.flink.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author: LexKaing
 * @create: 2019-06-19 22:06
 * @description:
 **/
public class LanguageAnalyzer {

    public static final Logger logger = LoggerFactory.getLogger(LanguageAnalyzer.class);
    public static LanguageAnalyzer INSTANCE;
    public static LanguageDetector languageDetector;

    private LanguageAnalyzer() {
        try {
            List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
            languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                    .withProfiles(languageProfiles).build();
        } catch (IOException e) {
            logger.error("LanguageAnalyzer IOException", e);
        }
    }

    public static LanguageAnalyzer getInstance() {
        synchronized (LanguageAnalyzer.class) {
            if (INSTANCE == null) {
                INSTANCE = new LanguageAnalyzer();
            }
            return INSTANCE;
        }
    }

    public static void main(String[] args) {
        String text = "I would honestly give Corey Norman my left leg if he asked for it wbu";
        System.out.println(LanguageAnalyzer.getInstance().detectLanguage(text));
    }

    // 识别文本语言
    public String detectLanguage(String text) {
        try {
            if (languageDetector == null) {
                List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
                languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                        .withProfiles(languageProfiles).build();
            }

            Optional<LdLocale> lang = languageDetector.detect(text);
            if (lang.isPresent()) {
                logger.error("language detect failed");
                return lang.get().toString();
            }

        } catch (IOException e) {
            logger.error("detectLanguage IOException", e);
        }
        return Constants.UNKNOWN;
    }
}