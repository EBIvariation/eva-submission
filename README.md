To run the downloader the command to use is:

java -jar eva-integration-0.0.1-SNAPSHOT.jar --assembly.download-root-path=[LOCAL_ASSEMBLY_ROOT_DIR] --assembly.accession=[ASSEMBLY_ACCESSION]

LOCAL_ASSEMBLY_ROOT_DIR is the path to a local directory where inner directories of this directory hold the FASTA files for that asssembly. e.g. if LOCAL_ASSEMBLY_ROOT_DIR is set as "/my/assembly/root/" and the assembly accession is GCA_000001405.23 then the fasta files for this assembly will be saved in the directory /my/assembly/root/GCA_000001405.23