package uk.ac.ebi.eva.fastadownload;

import uk.ac.ebi.eva.configuration.AssemblyDownloadProperties;
import uk.ac.ebi.eva.configuration.EnaFtpProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tom on 04/08/16.
 *
 * This class contains configuration, and integration flows for downloading a sequence report file from ENA's ftp
 * directories, then using the chromosome accessions in this file to query ENA's API to download the FASTA sequences for
 * this assembly to one file per chromosome.
 *
 */
@Configuration
public class ENAFastaDownload {

    @Autowired
    private ConfigurableApplicationContext appContext;

    @Autowired
    private EnaFtpProperties enaFtpProperties;
    
    @Autowired
    private AssemblyDownloadProperties assemblyDownloadProperties;

    @Autowired
    private SequenceReportPathTransformer sequenceReportPathTransformer;

    @Autowired
    private SequenceReportProcessor sequenceReportProcessor;


    // FOR NOW DOWNLOAD EVERYTHING, NO CHECKING FOR FILE EXISTENCE

//    @Bean
//    public IntegrationFlow entryFlow() {
//        return IntegrationFlows
//                .from("inputChannel")
//                .<String, Boolean>route(filepath -> new File(filepath).exists(), mapping -> mapping
//                    .subFlowMapping("false", sf -> sf
//                        .channel("channelIntoSeqRepDL")) // if sequence report file doesn't exist, then download it
//                    .subFlowMapping("true", sf -> sf
//                        .channel("channelIntoDownloadFasta"))) // if sequence report file does exist then use it to download fasta
//                .get();
//    }

    @Bean
    public Message starterMessage() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("sequenceReportLocalPath", assemblyDownloadProperties.getDownloadPath().toString());
        headers.put("enaFtpSequenceReportDir", enaFtpProperties.getSequenceReportRoot());
        headers.put("fastaLocal", assemblyDownloadProperties.getFastaDownloadPath());
        
//        return new GenericMessage<String>(integrationOptions.getString("assemblyAccession"), headers);
        return new GenericMessage<String>((String) headers.get("sequenceReportLocalPath"), headers);
    }

    private String getLocalFileName(){
        return assemblyDownloadProperties.getSequenceReportFileBasename().replaceAll(".txt", "_" + new Date().getTime() + ".txt");
    }

    /**
     * This integration flow receives a message with the payload as the path to the file name to where the sequence
     * report file from ENA will be downloaded.
     * The message is then transformed so the message payload is the remote path for the root of ENA's sequence report
     * directory. The reason this isn't the initial payload is because the application originally checked for the
     * existence of the sequence report file, and FASTA files, before continuing here- but this feature has been
     * temporarily removed.
     * The contents of the remote directory is queried and a sequence report file with the name including the string of
     * the assembly reference is filtered for.
     * This file is then downloaded to the previously mentioned local directory.
     * The integration flow then sends a message to the "channelIntoDownloadFasta" channel, with the local sequence
     * report file path as its payload.
     *
     * @return the integration flow used to download a sequence report file from ENA
     */
    @Bean
    public IntegrationFlow sequenceReportDownloadFlow() {
        return IntegrationFlows
                .from("inputChannel")
                .transform(m -> enaFtpProperties.getSequenceReportRoot())
                .handle(Ftp.outboundGateway(enaFtpSessionFactory(), "ls", "payload")
                        .options("-1 -R")
                )
                .split()
                .filter("payload.matches('[\\w\\/]*" + assemblyDownloadProperties.getSequenceReportFileBasename() + "')")
                .transform(sequenceReportPathTransformer, "transform")
                .handle(Ftp.outboundGateway(enaFtpSessionFactory(), "get", "payload")
                        .localDirectory(new File(assemblyDownloadProperties.getDownloadRootPath()))
                        .localFilename(f -> getLocalFileName())
                )
                .channel("channelIntoDownloadFasta")
                .get();
    }

    /**
     * This integration flow receives a message with a local path to an ENA sequence report file.
     * The chromosome accessions are read from the file and split into multiple messages, one chromosome accession per
     * message, with the chromosome accession as the payload.
     * Each message's header is enriched with the chromosome accession.
     * A task executor is used to process the chromosome accession messages in parallel.
     * ENA's API is queried with each chromosome accession, for the FASTA formatted sequence of that chromosome.
     * Each chromosome's FASTA string is output to a separate file.
     * After each chromosome's FASTA file has been output, the integration flow outputs to the shutdownChannel (which
     * shuts down the context).
     *
     * @return the integration flow used to download a fasta file from ENA
     */
    @Bean
    public IntegrationFlow fastaDownloadFlow() {
        return IntegrationFlows
                .from("channelIntoDownloadFasta")
                .transform(sequenceReportProcessor, "getChromosomeAccessions")
                .split()
                .enrichHeaders(s -> s.headerExpressions(h -> h
                        .put("chromAcc", "payload")))
                .channel(MessageChannels.executor(taskExecutor()))
                .handle(Http.outboundGateway("https://www.ebi.ac.uk/ena/data/view/{payload}&amp;display=fasta")
                        .httpMethod(HttpMethod.GET)
                        .expectedResponseType(java.lang.String.class)
                        .uriVariable("payload", "payload"))
                .channel(MessageChannels.queue(15))
                .handle(Files.outboundGateway(new File(assemblyDownloadProperties.getDownloadRootPath()))
                                .fileExistsMode(FileExistsMode.REPLACE)
                                .fileNameGenerator(message -> message.getHeaders().get("chromAcc") + ".fasta")
                                    ,e -> e.poller(Pollers.fixedDelay(100))
                )
                .aggregate()
                .<List<File>, String>transform(m -> m.get(0).getParent())
                .channel("shutdownChannel")
                .get();
    }

    @Bean
    public IntegrationFlow shutdown() {
        return IntegrationFlows
                .from("shutdownChannel")
                .handle(m -> appContext.close())
                .get();
    }

    @Bean
    public DefaultFtpSessionFactory enaFtpSessionFactory() {
        DefaultFtpSessionFactory sessionFactory = new DefaultFtpSessionFactory();
        sessionFactory.setHost(enaFtpProperties.getHost());
        sessionFactory.setPort(enaFtpProperties.getPort());
        sessionFactory.setUsername(enaFtpProperties.getUsername());
        sessionFactory.setPassword(enaFtpProperties.getPassword());
        return sessionFactory;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        return executor;
    }


}