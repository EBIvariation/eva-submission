package embl.ebi.variation.eva.seqrep_fasta_dl;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.integration.annotation.IntegrationComponentScan;
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
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tom on 04/08/16.
 */
@Configuration
@ComponentScan
@IntegrationComponentScan
public class ENASequenceReportDownload {

    @Autowired
    private ObjectMap integrationOptions;

    @Autowired
    private SeqRepPathTransformer seqRepPathTransformer;

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
    public Message starterMessage(){
        Map<String, Object> headers = new HashMap<>();
        headers.put("seqReportLocalPath", Paths.get(integrationOptions.getString("localAssemblyDir"),
                integrationOptions.getString("assemblyAccession") + "_sequence_report.txt").toString());
        headers.put("enaFtpSeqRepDir", integrationOptions.getString("enaFtpSeqRepRoot"));
        headers.put("fastaLocal", Paths.get(integrationOptions.getString("localAssemblyDir"),
                integrationOptions.getString("assemblyAccession") + ".fasta").toString());
        GenericMessage message = new GenericMessage<String>(integrationOptions.getString("assemblyAccession"), headers);

        return message;
    }

    @Bean
    public IntegrationFlow seqReportDownloadFlow() {
        return IntegrationFlows
                .from("inputChannel")
                .transform(m -> integrationOptions.getString("enaFtpSeqRepRoot"))
                .handle(Ftp.outboundGateway(enaFtpSessionFactory(), "ls", "payload")
                        .options("-1 -R")
                )
                .split()
                .filter("payload.matches('[\\w\\/]*" + integrationOptions.getString("sequenceReportFileBasename") + "')")
                .transform(seqRepPathTransformer, "transform")
                .handle(Ftp.outboundGateway(enaFtpSessionFactory(), "get", "payload")
                        .localDirectory(new File(integrationOptions.getString("localAssemblyRoot"))))
                .channel("channelIntoDownloadFasta")
                .get();
    }


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
                .handle(Files.outboundGateway(Paths.get(integrationOptions.getString("localAssemblyRoot"), integrationOptions.getString("assemblyAccession")).toFile())
                                .fileExistsMode(FileExistsMode.REPLACE)
                                .fileNameGenerator(message -> message.getHeaders().get("chromAcc") + ".fasta"),
                        e -> e.poller(Pollers.fixedDelay(1000))
                )
                .aggregate()
                .<List<File>, String>transform(m -> m.get(0).getParent())
                .handle(m -> System.out.println(m.getPayload()))
                .get();
    }

    @Bean
    public DefaultFtpSessionFactory enaFtpSessionFactory(){
        DefaultFtpSessionFactory sessionFactory = new DefaultFtpSessionFactory();
        sessionFactory.setHost(integrationOptions.getString("enaFtpHost"));
        sessionFactory.setPort(integrationOptions.getInt("enaFtpPort"));
        sessionFactory.setUsername(integrationOptions.getString("enaFtpUserId"));
        sessionFactory.setPassword(integrationOptions.getString("enaFtpPassword"));
        return sessionFactory;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        return executor;
    }


}
