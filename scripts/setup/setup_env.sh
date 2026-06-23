#!/usr/bin/env bash
set -e
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
python3 -m pip install --user -r requirements.txt
mkdir -p raw_data hdfs_output business_output
echo "Environment setup complete."
