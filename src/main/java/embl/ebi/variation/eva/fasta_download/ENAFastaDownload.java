package embl.ebi.variation.eva.fasta_download;

import embl.ebi.variation.eva.sequence_report_processing.SequenceReportProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.http.HttpMethod;
import org.springframework.integration.annotation.*;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.http.Http;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.http.outbound.HttpRequestExecutingMessageHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.File;
import java.util.Collections;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
public class ENAFastaDownload {

    @Bean
    public ThreadPoolTaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(15);
        return executor;
    }

    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private SequenceReportProcessor sequenceReportProcessor;

    @Bean
    public IntegrationFlow fastaDownloadFlow() {
        return IntegrationFlows.from("channelIntoDownloadFastaENA")
                .transform(sequenceReportProcessor, "getChromosomeAccessions")
                .split()
                .channel(MessageChannels.executor(taskExecutor))
                .handle(Http.outboundGateway("https://www.ebi.ac.uk/ena/data/view/{payload}&amp;display=fasta")
                        .httpMethod(HttpMethod.GET)
                        .expectedResponseType(java.lang.String.class)
                        .uriVariable("payload", "payload"))
                .channel(MessageChannels.queue(15))
                .handle(Files.outboundGateway(new File("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound"))
                        .fileExistsMode(FileExistsMode.APPEND)
                        .fileNameGenerator(message -> "GCA_000001405.10.fasta2"),
                    e -> e.poller(Pollers.fixedDelay(1000)))
                .channel("nullChannel")
                .get();

    }

}
