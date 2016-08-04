package embl.ebi.variation.eva;

import org.slf4j.LoggerFactory;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;

/**
 * Created by tom on 04/08/16.
 */
public class ENASequenceReportDL {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ENASequenceReportDL.class);

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx =
                new ClassPathXmlApplicationContext("ena-inbound-config.xml");

        PollableChannel ftpChannel = ctx.getBean("ftpChannel", PollableChannel.class);

        Message<?> message1 = ftpChannel.receive(2000);
        Message<?> message2 = ftpChannel.receive(2000);
        Message<?> message3 = ftpChannel.receive(1000);

        LOGGER.info(String.format("Received first file message: %s.", message1));
        LOGGER.info(String.format("Received second file message: %s.", message2));
        LOGGER.info(String.format("Received nothing else: %s.", message3));

        ctx.close();
    }

}
