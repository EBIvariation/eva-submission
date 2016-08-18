package embl.ebi.variation.eva.fasta_download;

import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

/**
 * Created by tom on 18/08/16.
 */
public class ChannelConfig {

    @Bean
    public MessageChannel channelToGetChromAccs() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelToSplitter() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelToHttpRequest() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelToFileOutput() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelOutFastaDownload(){
        return new DirectChannel();
    }

}
