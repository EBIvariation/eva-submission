package embl.ebi.variation.eva.sequence_report_download;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;

/**
 * Created by tom on 17/08/16.
 */
@Configuration(value = "seqReportChannelConfig")
@ComponentScan("embl.ebi.variation.eva.fasta_download")
@IntegrationComponentScan("embl.ebi.variation.eva.fasta_download")
@EnableIntegration
public class ChannelConfig {

    @Bean
    @Description("Channel to receive input, at the moment the input is to the directory")
    public MessageChannel channelIntoSeqReportDl(){
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelIntoSeqRepDlChain(){
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelOutSeqRepDlChain(){
        return new DirectChannel();
    }

}
