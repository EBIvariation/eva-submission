package embl.ebi.variation.eva.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.ftp.gateway.FtpOutboundGateway;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
@EnableIntegration
public class SequenceReportDownloadConfiguration {

    @Autowired
    DefaultFtpSessionFactory enaFtpSessionFactory;

    @Bean
    public FtpOutboundGateway lsSequenceReports(){
        FtpOutboundGateway outboundGateway = new FtpOutboundGateway(enaFtpSessionFactory, "ls", "payload");
        outboundGateway.setOptions("-1 -R");
        return outboundGateway;
    }

}
