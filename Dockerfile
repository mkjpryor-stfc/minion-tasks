#####
## Dockerfile for Minion image
#####

# Build wheels in a build stage with git and gcc available
FROM python:3.6-slim-jessie AS minion-build

RUN apt-get update && \
    apt-get install -y git build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy the application code in and build wheels
COPY . /application
RUN mkdir /pip-wheels && \
    pip wheel --wheel-dir /pip-wheels /application


# Build the actual container without gcc
FROM python:3.6-slim-jessie

# Create a local user to run Minion under
ENV MINION_USER minion
ENV MINION_GROUP minion
ENV MINION_UID 1001
ENV MINION_GID 1001
RUN groupadd -g $MINION_GID $MINION_GROUP && \
    useradd -M -g $MINION_GID -s /sbin/nologin -u $MINION_UID $MINION_USER

# Create the entrypoint script
RUN echo '#!/usr/bin/env bash' > /usr/local/bin/docker-entrypoint.sh && \
    echo 'exec "$@"' >> /usr/local/bin/docker-entrypoint.sh && \
    chmod +x /usr/local/bin/docker-entrypoint.sh

# Copy the wheels from the build container and install them
COPY --from=minion-build /pip-wheels  /pip-wheels
RUN pip install --no-index --find-links=/pip-wheels minion-workflows

USER $MINION_USER
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["minion"]
