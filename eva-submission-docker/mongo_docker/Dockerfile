FROM ubuntu:22.04

RUN apt update  \
    && apt install -y systemctl wget

# Copy script
COPY create_mongo_shard_cluster.sh /create_mongo_shard_cluster.sh

# Set executable permissions on the script
RUN chmod +x /create_mongo_shard_cluster.sh

# Expose ports for config server, config shard, and mongos
EXPOSE 27017 27018 27019