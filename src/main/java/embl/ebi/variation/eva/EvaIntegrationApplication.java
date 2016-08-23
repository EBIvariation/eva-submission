package embl.ebi.variation.eva;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

@SpringBootApplication
@IntegrationComponentScan
public class EvaIntegrationApplication {

    @Autowired
    private static ObjectMap integrationOptions;

    public static void main(String[] args) {

        ConfigurableApplicationContext ctx = SpringApplication.run(EvaIntegrationApplication.class, args);

        GenericMessage starterMessage = ctx.getBean("starterMessage", GenericMessage.class);
        MessageChannel inputChannel = ctx.getBean("inputChannel", MessageChannel.class);
        inputChannel.send(starterMessage);

//        ctx.close();
	}

}
