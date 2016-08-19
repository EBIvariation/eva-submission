package embl.ebi.variation.eva.seqrep_fasta_dl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.ftp.Ftp;
import org.springframework.integration.dsl.http.Http;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;

/**
 * Created by tom on 04/08/16.
 */
@Component
public class ENASequenceReportDownload {

    @Autowired
    private RouterConfig routerConfig;

    @Autowired
    private DefaultFtpSessionFactory sessionFactory;

    @Autowired
    private SequenceReportPathTransformer pathTransformer;

    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private SequenceReportProcessor sequenceReportProcessor;

    @Autowired
    private TransformerConfig transformerConfig;

    @Bean
    public IntegrationFlow seqReportDownloadFlow() {
        return IntegrationFlows
                .from("inputChannel")

//                .route(routerConfig, "seqReportRouter")
//                    .channel("channelIntoDownloadSeqRep")
                    .transform(transformerConfig, "changePayloadForEnaFtpSeqRepDir")
                    .handle(Ftp.outboundGateway(sessionFactory, "ls", "payload")
                            .options("-1 -R")
                    )
                    .split()
                    .filter("payload.matches('[\\w\\/]*GCA_000001405\\.10_sequence_report\\.txt')")
                    .transform(pathTransformer, "transform")
                    .handle(Ftp.outboundGateway(sessionFactory, "get", "payload")
                            .localDirectory(new File("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound")))
                    .channel("channelIntoDownloadFasta")
                .get();
    }


    @Bean
    public IntegrationFlow fastaDownloadFlow() {
        return IntegrationFlows
                .from("channelIntoDownloadFasta")

////                .route(routerConfig, "fastaRouter")
////                    .channel("channelIntoDownloadFasta")
//                    .transform(transformerConfig, "changePayloadForSeqReportLocalPath")
                    .transform(sequenceReportProcessor, "getChromosomeAccessions")
                    .split()
                    .channel(MessageChannels.executor(taskExecutor))
                    .handle(Http.outboundGateway("https://www.ebi.ac.uk/ena/data/view/{payload}&amp;display=fasta")
                            .httpMethod(HttpMethod.GET)
                            .expectedResponseType(java.lang.String.class)
                            .uriVariable("payload", "payload"))
                    .resequence(r -> r.releasePartialSequences(true)) // put fasta sequences in order
                    .channel(MessageChannels.queue(15))
                    .handle(Files.outboundGateway(new File("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound"))
                                    .fileExistsMode(FileExistsMode.APPEND)
                                    .fileNameGenerator(message -> "GCA_000001405.10.fasta2"),
                            e -> e.poller(Pollers.fixedDelay(1000))
                    )
                .aggregate()
                .<List<File>, File>transform(m -> m.get(0))
                .handle(m -> System.out.println(m.getPayload().toString()))
                .get();
    }


}
