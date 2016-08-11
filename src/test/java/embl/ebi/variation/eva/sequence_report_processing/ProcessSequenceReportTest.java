package embl.ebi.variation.eva.sequence_report_processing;

import embl.ebi.variation.eva.utils.TestProperties;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by tom on 11/08/16.
 */
public class ProcessSequenceReportTest {

    private Logger logger = Logger.getLogger(ProcessSequenceReport.class);

    private static final String TEST_SEQUENCE_REPORT_FILENAME = "/GCA_000001405.23_sequence_report.txt";

    @Test
    public void getChromosomeAccessionsTest() throws Exception {

        TestProperties testProperties = TestProperties.getInstance();
        String[] testChromosomeAccessions = testProperties.properties.getProperty("seqreport.accessions").split(",");

        ProcessSequenceReport processSequenceReport = new ProcessSequenceReport();
        URL testSequenceReportFilePath = ProcessSequenceReportTest.class.getResource(TEST_SEQUENCE_REPORT_FILENAME);
        File testSequenceReportFile = new File(testSequenceReportFilePath.getFile());
        List<String> chromosomeAccessions = processSequenceReport.getChromosomeAccessions(testSequenceReportFile);

        Collections.sort(chromosomeAccessions);

        Assert.assertThat(chromosomeAccessions, IsIterableContainingInAnyOrder.containsInAnyOrder(testChromosomeAccessions));


    }

}