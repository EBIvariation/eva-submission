FROM ubuntu:22.04

RUN apt update  \
    && apt install -y systemctl wget \

# Copy startup script
COPY start.sh /start.sh

# Set executable permissions on the startup script
RUN chmod +x /start.sh

# Expose ports for config server, config shard, and mongos
EXPOSE 27017 27018 27019

# Run the startup script
#CMD ["/start.sh", "4.0.28"]
