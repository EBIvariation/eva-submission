package embl.ebi.variation.eva.fasta_download;

import org.springframework.context.annotation.Bean;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.http.HttpMethod;
import org.springframework.integration.http.outbound.HttpRequestExecutingMessageHandler;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * Created by tom on 18/08/16.
 */
@Component
public class ENAHttpFastaDownload {

    @Bean(name = "enaFastaHttpMessageHandler")
    public HttpRequestExecutingMessageHandler httpMessageHandler(){
        HttpRequestExecutingMessageHandler handler = new HttpRequestExecutingMessageHandler("https://www.ebi.ac.uk/ena/data/view/{chromAcc}&amp;display=fasta");
        handler.setHttpMethod(HttpMethod.GET);
        handler.setExpectedResponseType(java.lang.String.class);
        SpelExpressionParser parser = new SpelExpressionParser();
        handler.setUriVariableExpressions(Collections.singletonMap("chromAcc", parser.parseExpression("payload")));
        return handler;
    }
}
