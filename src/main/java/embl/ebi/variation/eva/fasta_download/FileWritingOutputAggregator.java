package embl.ebi.variation.eva.fasta_download;

import org.apache.log4j.Logger;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by tom on 18/08/16.
 */
@Component("fileWritingOutputAggregator")
public class FileWritingOutputAggregator {

    private Logger logger = Logger.getLogger(FileWritingOutputAggregator.class);

    public void aggregate(List<Message> messages){
        if (messages.size() > 0){
            logger.info("MESSAGE LIST SIZE: " + Integer.toString(messages.size()));
        }
    }

//    @Aggregator(inputChannel = "channelOutFastaDownload", outputChannel = "nullchannel", autoStartup = "false")
//    public void aggregateFileOutputGatewayMessages(List<Message> messages) {
//        if (messages.size() > 0){
//            logger.info("MESSAGE LIST SIZE: " + Integer.toString(messages.size()));
//        }
//    }

}
