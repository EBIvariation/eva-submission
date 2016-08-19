package embl.ebi.variation.eva.utils;

import embl.ebi.variation.eva.sequence_report_download.ENASequenceReportDownload;
import embl.ebi.variation.eva.sequence_report_download.SequenceReportProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by tom on 11/08/16.
 */
public class TestProperties {

    private Logger logger = Logger.getLogger(SequenceReportProcessor.class);

    public Properties properties;

    private TestProperties(){

        properties = new Properties();
        try {
            properties.load(ENASequenceReportDownload.class.getClassLoader().getResourceAsStream("test.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static class TestPropertiesHolder {
        private static final TestProperties INSTANCE = new TestProperties();
    }

    public static TestProperties getInstance() {
        return TestPropertiesHolder.INSTANCE;
    }

}
