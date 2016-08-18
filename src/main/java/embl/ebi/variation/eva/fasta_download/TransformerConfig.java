package embl.ebi.variation.eva.fasta_download;

import embl.ebi.variation.eva.sequence_report_processing.SequenceReportProcessor;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;

/**
 * Created by tom on 18/08/16.
 */
@Component
public class TransformerConfig {

    private SequenceReportProcessor sequenceReportProcessor = new SequenceReportProcessor();

    @Transformer(inputChannel = "channelIntoDownloadFastaENA", outputChannel = "channelToSplitter")
    public List<String> getChromosomeAccessionsFromFile(Message<File> msg){
        return sequenceReportProcessor.getChromosomeAccessions(msg.getPayload());
    }
}
