package embl.ebi.variation.eva.sequence_report_processing;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 11/08/16.
 */
public class SequenceReportProcessor {

    private Logger logger = Logger.getLogger(SequenceReportProcessor.class);

    public String getChromosomeAccessionsString(File file){
        List<String> chromosomeAccessionsList = this.getChromosomeAccessions(file);
        return StringUtils.join(chromosomeAccessionsList, ",");
    }

    public List<String> getChromosomeAccessions(File file){
        logger.info("Getting chromosome accessions from sequence report file: " + file);

        CSVParser parser = setUpCSVParser(file);

        List<String> chromosomeAccessions = parseChromosomeAccessions(parser);

        return chromosomeAccessions;
    }

    private CSVParser setUpCSVParser(File file){
        CSVParser parser = null;

        try {
            parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.TDF);
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        return parser;
    }

    private List<String> parseChromosomeAccessions(CSVParser parser){
        List<String> chromsomeAccessions = new ArrayList<String>();

        parser.forEach(csvRecord -> chromsomeAccessions.add(csvRecord.get(0)));

        chromsomeAccessions.remove(0); // remove the header element

        return chromsomeAccessions;
    }


}
