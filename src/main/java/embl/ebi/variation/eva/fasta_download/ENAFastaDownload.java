package embl.ebi.variation.eva.fasta_download;

import embl.ebi.variation.eva.sequence_report_processing.SequenceReportProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.http.HttpMethod;
import org.springframework.integration.annotation.*;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.http.outbound.HttpRequestExecutingMessageHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
public class ENAFastaDownload {

//    @Autowired
//    ThreadPoolTaskExecutor taskExecutor;
//
    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata poller(){
        return Pollers
                .fixedDelay(100)
                .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "channelToHttpRequest")
    public HttpRequestExecutingMessageHandler httpMessageHandler(){
        HttpRequestExecutingMessageHandler handler = new HttpRequestExecutingMessageHandler("https://www.ebi.ac.uk/ena/data/view/{chromAcc}&amp;display=fasta");
        handler.setHttpMethod(HttpMethod.GET);
        handler.setExpectedResponseType(java.lang.String.class);
        SpelExpressionParser parser = new SpelExpressionParser();
        handler.setUriVariableExpressions(Collections.singletonMap("chromAcc", parser.parseExpression("payload")));
        handler.setOutputChannelName("channelToFileOutput");
        return handler;
    }

    @Bean
    @ServiceActivator(inputChannel = "channelToFileOutput")
    public FileWritingMessageHandler fileWritingMessageHandler(){
        FileWritingMessageHandler handler = new FileWritingMessageHandler(new File("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound"));
        handler.setFileExistsMode(FileExistsMode.APPEND);
        handler.setFileNameGenerator(message -> "GCA_000001405.10.fasta2");
        handler.setOutputChannelName("nullChannel");
        return handler;
    }

//    @Bean
//    public ThreadPoolTaskExecutor taskExecutor(){
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//        executor.setCorePoolSize(5);
//        executor.setMaxPoolSize(15);
//        return executor;
//    }



//    @Bean
//    public IntegrationFlow fastaDownloadFlow() {
//        return IntegrationFlows.from("channelIntoDownloadFastaENA")
//                .transform(sequenceReportProcessor, "getChromosomeAccessions")
//                .split()
//                .handle("enaFastaHttpMessageHandler", "handleMessage")
////                .handle(Http.outboundGateway("https://www.ebi.ac.uk/ena/data/view/{payload}&amp;display=fasta")
////                        .httpMethod(HttpMethod.GET)
////                        .expectedResponseType(java.lang.String.class)
////                        .uriVariable("payload", "payload"))
//                .handle(Files.outboundGateway(new File("/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound"))
//                        .fileExistsMode(FileExistsMode.APPEND)
//                        .fileNameGenerator(message -> "GCA_000001405.10.fasta2"))
//                .channel("nullChannel")
//                .get();
//
//    }

}
