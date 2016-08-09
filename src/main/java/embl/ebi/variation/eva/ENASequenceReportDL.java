package embl.ebi.variation.eva;

import static org.junit.Assert.assertTrue;

import org.slf4j.LoggerFactory;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

import java.util.Properties;

/**
 * Created by tom on 04/08/16.
 */
public class ENASequenceReportDL {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ENASequenceReportDL.class);


    public static void downloadSequenceReport(ConfigurableApplicationContext ctx, String assemblyAccession, String sequenceReportDirectory) {

        setEnvironmentForAccession(ctx, assemblyAccession);

        ctx.refresh();

        MessageChannel inputChannel = ctx.getBean("inputChannel", MessageChannel.class);
        PollableChannel outputChannel = ctx.getBean("outputChannel", PollableChannel.class);
        inputChannel.send(new GenericMessage<String>(sequenceReportDirectory));

//        assertTrue(outputChannel.receive().getPayload());

        ctx.close();
    }

    private static void setEnvironmentForAccession(ConfigurableApplicationContext ctx, String accession) {
        StandardEnvironment env = new StandardEnvironment();
        Properties props = new Properties();
        props.setProperty("assembly.accession", accession);
        PropertiesPropertySource pps = new PropertiesPropertySource("ftpprops", props);
        env.getPropertySources().addFirst(pps);
        ctx.setEnvironment(env);
    }

}
