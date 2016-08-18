package embl.ebi.variation.eva.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
@EnableIntegration
public class InfrastructureConfiguration {

    // DOWNLOAD SEQUENCE REPORT

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
//
//    // DOWNLOAD FASTA FILE
//
//    public MessageChannel channelIntoDownloadFastaENA(){
//        return new DirectChannel();
//    }
//
//    @Bean
//    public MessageChannel channelOutGetChromAccsSplit(){
//        return new DirectChannel();
//    }
//
//    @Bean
//    public MessageChannel enaFastaReply(){
//        return new DirectChannel();
//    }

}
