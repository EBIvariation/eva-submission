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

    @Value("${ena.ftp.sequence_report_path}") private String enaFtpSeqRepPath;

    private ObjectMap enaFtpOptions  = new ObjectMap();

    private void loadEnaFtpOptions(){
        enaFtpOptions.put("enaFtpUserId", enaFtpUserId);
        enaFtpOptions.put("enaFtpPassword", enaFtpPassword);
        enaFtpOptions.put("enaFtpPort", enaFtpPort);
        enaFtpOptions.put("enaFtpHost", enaFtpHost);

        logger.debug("Using as enaFtpOptions: {}", enaFtpOptions.entrySet().toString());
    }

    public ObjectMap getEnaFtpOptions() {
        loadEnaFtpOptions();
        return enaFtpOptions;
    }

    public void loadArgs() {
        logger.info("Load args");
        loadEnaFtpOptions();
    }

}
