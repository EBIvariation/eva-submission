package embl.ebi.variation.eva.seqrep_fasta_dl;

import embl.ebi.variation.eva.config.EvaIntegrationArgsConfig;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
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
import org.springframework.integration.jdbc.JdbcMessageStore;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by tom on 04/08/16.
 */
@Component
public class ENASequenceReportDownload {

    @Autowired
    private ObjectMap integrationOptions;

    @Value("${assembly.accession}")
    private String assemblyAccession;

    @Value("${assembly.accession}#{integrationOptions.getString(\"sequenceReportFileSuffix\")}")
    private String sequenceReportFileBasename;

    @Value("#{integrationOptions.getString(\"enaFtpSeqRepRoot\")}")
    private String enaFtpSeqRepRoot;

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

                    .transform(m -> enaFtpSeqRepRoot)
                    .handle(Ftp.outboundGateway(sessionFactory, "ls", "payload")
                            .options("-1 -R")
                    )
                    .split()
                    .filter("payload.matches('[\\w\\/]*" + sequenceReportFileBasename + "')")
                    .transform(pathTransformer, "transform")
                    .handle(Ftp.outboundGateway(sessionFactory, "get", "payload")
                            .localDirectory(new File(integrationOptions.getString("localAssemblyRoot"))))
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
                    .enrichHeaders(s -> s.headerExpressions(h -> h
                            .put("chromAcc", "payload")))
                    .channel(MessageChannels.executor(taskExecutor))
                    .handle(Http.outboundGateway("https://www.ebi.ac.uk/ena/data/view/{payload}&amp;display=fasta")
                            .httpMethod(HttpMethod.GET)
                            .expectedResponseType(java.lang.String.class)
                            .uriVariable("payload", "payload"))
                    .channel(MessageChannels.queue(15))
                    .handle(Files.outboundGateway(Paths.get(integrationOptions.getString("localAssemblyRoot"), assemblyAccession).toFile())
                                    .fileExistsMode(FileExistsMode.REPLACE)
                                    .fileNameGenerator(message -> message.getHeaders().get("chromAcc") + ".fasta"),
                            e -> e.poller(Pollers.fixedDelay(1000))
                    )
                .aggregate()
                .<List<File>, String>transform(m -> m.get(0).getParent())
                .handle(m -> System.out.println(m.getPayload()))
                .get();
    }


}
