package embl.ebi.variation.eva;

import org.slf4j.LoggerFactory;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by tom on 04/08/16.
 */
public class ENASequenceReportDL {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ENASequenceReportDL.class);


    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext(
                new String[] { "ena-inbound-config.xml" },
                false);

        ctx.refresh();

//        ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext(
//                "classpath:ena-inbound-config.xml");

        final ToFtpFlowGateway toFtpFlow = ctx.getBean(ToFtpFlowGateway.class);

        toFtpFlow.lsFilesRecursive("/pub/databases/ena/assembly/");
//
//        for (String fileName : lsResults){
//            System.out.println(String.format("FILENAME: %s", fileName));
//        }







        ///////

//        PollableChannel ftpChannel = createNewChannel(ctx, "GCA_000001405.10");
//
//        Message<?> message1 = ftpChannel.receive(2000);
//
//        LOGGER.info(String.format("Received first file message: %s.", message1));

        ctx.close();
    }

    private static PollableChannel createNewChannel(ConfigurableApplicationContext ctx, String accession) {
//        ctx = new ClassPathXmlApplicationContext(
//                new String[] { "ena-inbound-config.xml" },
//                false);
        setEnvironmentForAccession(ctx, accession);
        ctx.refresh();
        PollableChannel channel = ctx.getBean("ftpChannel", PollableChannel.class);
        return channel;
    }

    private static void setEnvironmentForAccession(ConfigurableApplicationContext ctx, String accession) {
        StandardEnvironment env = new StandardEnvironment();
        Properties props = new Properties();
        // populate properties for customer
        props.setProperty("remote.directory", "/pub/databases/ena/assembly/GCA_000/GCA_000001/");
        props.setProperty("filename.pattern", String.format("%s_sequence_report.txt", accession));


        PropertiesPropertySource pps = new PropertiesPropertySource("ftpprops", props);
//        env.getPropertySources().addLast(pps);
        env.getPropertySources().addFirst(pps);
        ctx.setEnvironment(env);
    }

}
