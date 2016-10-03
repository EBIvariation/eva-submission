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

    public TestProperties() throws IOException {

        properties = new Properties();
        properties.load(ENAFastaDownload.class.getClassLoader().getResourceAsStream("test.properties"));
    }

    public Properties getProperties() {
        return properties;
    }

}
