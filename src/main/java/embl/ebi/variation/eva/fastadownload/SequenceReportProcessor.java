package embl.ebi.variation.eva.fastadownload;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 11/08/16.
 */
@Component
public class SequenceReportProcessor {

    private Logger logger = Logger.getLogger(SequenceReportProcessor.class);

    public List<String> getChromosomeAccessions(File file) throws IOException {
        logger.info("Getting chromosome accessions from sequence report file: " + file);
        CSVParser parser = setUpCSVParser(file);
        List<String> chromosomeAccessions = parseChromosomeAccessions(parser);
        return chromosomeAccessions;
    }

    private CSVParser setUpCSVParser(File file) throws IOException {
        return CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.TDF);
    }

    private List<String> parseChromosomeAccessions(CSVParser parser){
        List<String> chromosomeAccessions = new ArrayList<String>();
        parser.forEach(csvRecord -> chromosomeAccessions.add(csvRecord.get(0)));
        chromosomeAccessions.remove(0); // remove the header element
        return chromosomeAccessions;
    }


}
