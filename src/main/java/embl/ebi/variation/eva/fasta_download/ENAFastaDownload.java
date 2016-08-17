package embl.ebi.variation.eva.fasta_download;

import embl.ebi.variation.eva.sequence_report_processing.ProcessSequenceReport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.ftp.Ftp;
import org.springframework.integration.dsl.http.Http;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.File;

/**
 * Created by tom on 17/08/16.
 */
@Component
public class ENAFastaDownload {

    private ProcessSequenceReport processSequenceReport = new ProcessSequenceReport();

//    @MessagingGateway
//    public interface FastaDownload {
//        @Gateway(requestChannel = "fastaDownloadFlow.input")
//        void downloadFasta(String chromAcc);
//    }

    @Bean
    public IntegrationFlow fastaDownloadFlow() {
        return IntegrationFlows.from("channelIntoDownloadFastaENA")
                .transform(processSequenceReport, "getChromosomeAccessions")
                .split()
                .handle(Http.outboundGateway("https://www.ebi.ac.uk/ena/data/view/{payload}&amp;display=fasta")
                        .httpMethod(HttpMethod.GET)
                        .expectedResponseType(String.class)
                        .uriVariable("payload", "payload"))
                .handle(Files.outboundGateway("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound")
                        .fileExistsMode(FileExistsMode.APPEND)
                        .fileNameExpression("payload+'.fasta2'"))
                .get();

    }

}
