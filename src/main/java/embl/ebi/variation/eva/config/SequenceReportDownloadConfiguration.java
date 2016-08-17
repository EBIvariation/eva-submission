package embl.ebi.variation.eva.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.ftp.gateway.FtpOutboundGateway;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
@EnableIntegration
public class SequenceReportDownloadConfiguration {

    @Bean
    public FtpOutboundGateway lsSequenceReports(){
        FtpOutboundGateway outboundGateway = new FtpOutboundGateway();
    }


}
