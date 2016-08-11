package embl.ebi.variation.eva.sequence_report_processing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 11/08/16.
 */
public class ProcessSequenceReport {

    private Logger logger = Logger.getLogger(ProcessSequenceReport.class);

    public List getChromosomeAccessions(File file){
        logger.info("Getting chromosome accessions from sequence report file: " + file);

        CSVParser parser;

        try {
            parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.TDF);
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        List chromosomeAccessions = parseChromosomeAccessions(parser);

    }

    private List parseChromosomeAccessions(CSVParser parser){
        List chromsomeAccessions = new ArrayList();

        for (CSVRecord csvRecord : parser){

        }
    }


}
