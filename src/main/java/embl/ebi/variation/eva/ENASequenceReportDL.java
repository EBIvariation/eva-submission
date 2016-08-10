package embl.ebi.variation.eva;

import org.slf4j.LoggerFactory;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * Created by tom on 04/08/16.
 */
public class ENASequenceReportDL {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ENASequenceReportDL.class);


    public static void downloadSequenceReport(ConfigurableApplicationContext ctx, String assemblyAccession, String sequenceReportDirectory) {

        MessageChannel inputChannel = ctx.getBean("channelIntoSeqReportDl", MessageChannel.class);
        inputChannel.send(new GenericMessage<String>(sequenceReportDirectory));
    }

}
