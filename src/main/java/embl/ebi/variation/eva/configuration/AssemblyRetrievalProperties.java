package embl.ebi.variation.eva.configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "assembly", ignoreUnknownFields = false)
public class AssemblyRetrievalProperties {

	@NotNull
	@Pattern(regexp="GCA\\d+[.\\d+]") // TODO check this regex is fine
	private String accession;

	@NotNull
	private String downloadRootPath;
	
	
	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}

	public String getDownloadRootPath() {
		return downloadRootPath;
	}

	public void setDownloadRootPath(String downloadRootPath) {
		this.downloadRootPath = downloadRootPath;
	}

	public String getSequenceReportFileBasename() {
		return accession + "_sequence_report.txt";
	}

	public Path getDownloadPath() {
		return Paths.get(downloadRootPath, accession);
	}
	
	public Path getFastaDownloadPath() {
		return Paths.get(downloadRootPath, accession + ".fasta");
	}
}
