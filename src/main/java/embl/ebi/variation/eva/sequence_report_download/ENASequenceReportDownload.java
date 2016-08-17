package embl.ebi.variation.eva.sequence_report_download;

import embl.ebi.variation.eva.config.ConnectionConfig;
import embl.ebi.variation.eva.config.SequenceReportDownloadConfiguration;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.ftp.Ftp;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * Created by tom on 04/08/16.
 */
@Component
public class ENASequenceReportDownload {

    @Autowired
    private DefaultFtpSessionFactory sessionFactory;

    @Autowired
    private SequenceReportPathTransformer pathTransformer;

    @Autowired
    private MessageChannel channelOutSeqRepDlChain;

    @MessagingGateway
    public interface ENAFtpLs {
        @Gateway(requestChannel = "ftpOutboundGatewayFlow.input")
        void lsEnaFtp(String remoteDir);
    }

    @Bean
    public IntegrationFlow ftpOutboundGatewayFlow() {
        return f -> f
                .handle(Ftp.outboundGateway(sessionFactory, "ls", "payload")
                        .options("-1 -R")
//                        .regexFileNameFilter("GCA_000001405\\.10_sequence_report\\.txt")
                )
                .split()
                .filter("payload.matches('[\\w\\/]*GCA_000001405\\.10_sequence_report\\.txt')")
                .transform(pathTransformer, "transform")
                .handle(Ftp.outboundGateway(sessionFactory, "get", "payload")
                        .localDirectory(new File("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound")))
                .channel("fastaDownloadFlow.input");
    }


}
