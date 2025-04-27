FROM openjdk:11-jdk-slim

# Installation de SBT
RUN apt-get update && \
    apt-get install -y curl && \
    curl -L -o sbt-1.5.5.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-1.5.5.deb && \
    dpkg -i sbt-1.5.5.deb && \
    rm sbt-1.5.5.deb && \
    apt-get update && \
    apt-get install -y sbt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier les fichiers de build
COPY build.sbt ./
COPY project/build.properties project/plugins.sbt ./project/

# Copier le code source
COPY src ./src

# Créer le dossier de données et debug
RUN mkdir -p data/debug

# Compiler le code
RUN sbt compile

CMD ["sh", "-c", "sbt --batch \"runMain producer.MainProducer\""]
# CMD ["sbt", "runMain", "producer.MainProducer"]