package embl.ebi.variation.eva.utils;

import embl.ebi.variation.eva.fastadownload.ENAFastaDownload;
import embl.ebi.variation.eva.fastadownload.SequenceReportProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by tom on 11/08/16.
 */
public class TestProperties {

    private Logger logger = Logger.getLogger(SequenceReportProcessor.class);

    private Properties properties;

    public TestProperties(){

        properties = new Properties();
        try {
            properties.load(ENAFastaDownload.class.getClassLoader().getResourceAsStream("test.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public Properties getProperties() {
        return properties;
    }

}
