package embl.ebi.variation.eva.sequence_report_processing;

import embl.ebi.variation.eva.fastadownload.SequenceReportProcessor;
import embl.ebi.variation.eva.utils.TestProperties;
import org.apache.log4j.Logger;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * Created by tom on 11/08/16.
 */
public class SequenceReportProcessorTest {

    private Logger logger = Logger.getLogger(SequenceReportProcessor.class);

    private static final String TEST_SEQUENCE_REPORT_FILENAME = "/GCA_000001405.23_sequence_report.txt";

    @Test
    public void getChromosomeAccessionsTest() throws Exception {

        TestProperties testProperties = TestProperties.getInstance();
        String[] testChromosomeAccessions = testProperties.properties.getProperty("seqreport.accessions").split(",");

        SequenceReportProcessor sequenceReportProcessor = new SequenceReportProcessor();
        URL testSequenceReportFilePath = SequenceReportProcessorTest.class.getResource(TEST_SEQUENCE_REPORT_FILENAME);
        File testSequenceReportFile = new File(testSequenceReportFilePath.getFile());
        List<String> chromosomeAccessions = sequenceReportProcessor.getChromosomeAccessions(testSequenceReportFile);

        Collections.sort(chromosomeAccessions);

        Assert.assertThat(chromosomeAccessions, IsIterableContainingInAnyOrder.containsInAnyOrder(testChromosomeAccessions));

    }

}
