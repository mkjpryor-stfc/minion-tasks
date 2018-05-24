#####
## Dockerfile for creating Minion container images
#####

FROM python:3.6-slim

# Create a local user to run Minion under
ENV MINION_USER minion
ENV MINION_GROUP minion
ENV MINION_UID 1001
ENV MINION_GID 1001
RUN groupadd -g $MINION_GID $MINION_GROUP && \
    useradd -M -g $MINION_GID -s /sbin/nologin -u $MINION_UID $MINION_USER

# Create the entrypoint script
RUN echo "#!/usr/bin/env bash" > /usr/local/bin/docker-entrypoint.sh && \
    echo 'exec minion "$@"' >> /usr/local/bin/docker-entrypoint.sh && \
    chmod +x /usr/local/bin/docker-entrypoint.sh

# Install git for version detection
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Install Minion from the current directory
COPY . /application
RUN pip install -e /application

USER $MINION_USER
ENTRYPOINT ["minion"]
CMD ["--help"]
