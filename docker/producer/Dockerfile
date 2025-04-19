FROM openjdk:11-jdk-slim

RUN apt-get update && \
    apt-get install -y wget unzip && \
    rm -rf /var/lib/apt/lists/*

# Définir les variables d'environnement
ENV SCALA_VERSION=2.12.15 \
    SBT_VERSION=1.8.0

# Installer SBT
RUN wget https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz && \
    tar -xzf sbt-${SBT_VERSION}.tgz && \
    rm sbt-${SBT_VERSION}.tgz && \
    mv sbt /usr/local

# Ajouter SBT au PATH
ENV PATH="/usr/local/sbt/bin:${PATH}"

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers build.sbt et project
COPY build.sbt .
COPY project project/

# Résoudre les dépendances SBT (cache layer)
RUN sbt update

# Copier le reste des fichiers source
COPY src src/

# Compiler l'application
RUN sbt compile

# Point d'entrée
ENTRYPOINT ["sbt", "run"]