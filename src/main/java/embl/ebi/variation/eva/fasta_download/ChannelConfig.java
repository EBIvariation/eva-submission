package embl.ebi.variation.eva.fasta_download;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Created by tom on 18/08/16.
 */
@Configuration(value = "fastaChannelConfig")
@ComponentScan("embl.ebi.variation.eva.fasta_download")
@IntegrationComponentScan("embl.ebi.variation.eva.fasta_download")
@EnableIntegration
//@Import({ENAFastaDownload.class, SplitterConfig.class, TransformerConfig.class})
public class ChannelConfig {

    @Bean
    public ThreadPoolTaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(15);
        return executor;
    }

    @Bean
    public MessageChannel channelToGetChromAccs() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelToSplitter() {
        return new DirectChannel();
    }

    @Bean
    public ExecutorChannel channelToHttpRequest() {
        return new ExecutorChannel(taskExecutor());
    }

//    @Bean
//    public MessageChannel channelToHttpRequest() {
//        return new QueueChannel(1000);
//    }

    @Bean
    public QueueChannel channelToFileOutput() {
        return new QueueChannel(15);
    }

    @Bean
    public MessageChannel channelOutFastaDownload(){
        return new DirectChannel();
    }

}
