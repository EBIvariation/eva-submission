package embl.ebi.variation.eva.config;

import org.opencb.datastore.core.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by tom on 09/08/16.
 */
@Component
public class EVAIntegrationArgs {

    private static final Logger logger = LoggerFactory.getLogger(EVAIntegrationArgs.class);

    //// ftp connection
    @Value("${ena.ftp.userid}") private String enaFtpUserId;
    @Value("${ena.ftp.password}") private String enaFtpPassword;
    @Value("${ena.ftp.port:21}") private int enaFtpPort;
    @Value("${ena.ftp.host}") private String enaFtpHost;

    @Value("${ena.ftp.sequence_report_root}") private String enaFtpSeqRepRoot;

    //// sequence report file

    @Value("${sequence_report.file.suffix}") private String sequenceReportFileSuffix;

    //// local files

    @Value("${local.assembly.root}") private String localAssemblyRoot;


    private ObjectMap integrationOptions = new ObjectMap();

    private void loadIntegrationOptions(){
        integrationOptions.put("enaFtpUserId", enaFtpUserId);
        integrationOptions.put("enaFtpPassword", enaFtpPassword);
        integrationOptions.put("enaFtpPort", enaFtpPort);
        integrationOptions.put("enaFtpHost", enaFtpHost);
        integrationOptions.put("enaFtpSeqRepRoot", enaFtpSeqRepRoot);

        integrationOptions.put("sequenceReportFileSuffix", sequenceReportFileSuffix);

        integrationOptions.put("localAssemblyRoot", localAssemblyRoot);


        logger.debug("Using as integrationOptions: {}", integrationOptions.entrySet().toString());
    }

    public ObjectMap getIntegrationOptions() {
        loadIntegrationOptions();
        return integrationOptions;
    }

    public void loadArgs() {
        logger.info("Load args");
        loadIntegrationOptions();
    }

}
