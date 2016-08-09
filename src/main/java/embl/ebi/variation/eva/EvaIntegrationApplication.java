package embl.ebi.variation.eva;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.util.Properties;

public class EvaIntegrationApplication {

	public static void main(String[] args) {

		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext(
				new String[] {"eva-integration-config.xml"},
				false);

		Properties properties = loadProperties();

		ENASequenceReportDL.downloadSequenceReport(ctx, "GCA_000001405.10",
				properties.getProperty("sequenceReportDirectory"));
	}


	private static Properties loadProperties(){
		Properties prop = new Properties();
		try {
			prop.load(ENASequenceReportDL.class.getClassLoader().getResourceAsStream("user.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}
}
