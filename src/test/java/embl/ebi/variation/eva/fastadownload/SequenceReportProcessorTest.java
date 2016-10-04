package embl.ebi.variation.eva.fastadownload;

import embl.ebi.variation.eva.utils.TestProperties;
import org.apache.log4j.Logger;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.*;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by tom on 11/08/16.
 */
public class SequenceReportProcessorTest {

    private Logger logger = Logger.getLogger(SequenceReportProcessor.class);

    private static final String TEST_SEQUENCE_REPORT_FILENAME = "/GCA_000001405.23_sequence_report.txt";

    private List<String> chromosomeAccessions;

    @BeforeClass
    public void setUp() throws IOException {
        SequenceReportProcessor sequenceReportProcessor = new SequenceReportProcessor();
        URL testSequenceReportFilePath = SequenceReportProcessorTest.class.getResource(TEST_SEQUENCE_REPORT_FILENAME);
        File testSequenceReportFile = new File(testSequenceReportFilePath.getFile());
        chromosomeAccessions = sequenceReportProcessor.getChromosomeAccessions(testSequenceReportFile);
    }

    @Test
    public void getChromosomeAccessionsValidTest() throws Exception {
        String[] validChromosomeAccessions = {"CM000663.2", "CM000664.2", "CM000665.2", "CM000666.2"};
        List<String> validChromosomeAccessionsList = Arrays.asList(validChromosomeAccessions);
        Assert.assertTrue(chromosomeAccessions.containsAll(validChromosomeAccessionsList));
    }

    @Test
    public void getChromosomeAccessionsInvalidTest() throws Exception {
        String[] invalidChromosomeAccessions = {"JH159135.1", "GL582974.1", "CM000663.1", "CM000664.1"};
        List<String> invalidChromosomeAccessionsList = Arrays.asList(invalidChromosomeAccessions);
        Assert.assertFalse(chromosomeAccessions.containsAll(invalidChromosomeAccessionsList));
    }

}
