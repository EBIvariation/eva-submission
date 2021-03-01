#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/nextflow"

# Builds fake Java jars
cwd=${PWD}
cd ${SCRIPT_DIR}/java

javac FakeAccessionPipeline.java
jar cfe accession.jar FakeAccessionPipeline FakeAccessionPipeline.class

javac FakeVariantLoadPipeline.java
jar cfe variant-load.jar FakeVariantLoadPipeline FakeVariantLoadPipeline.class

cd ${cwd}
