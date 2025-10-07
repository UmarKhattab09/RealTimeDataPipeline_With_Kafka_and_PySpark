FROM apache/airflow:2.9.3

# Switch to root user for installation
USER root

# Install wget and clean up
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* || { echo "Failed to install wget"; exit 1; }

# Download OpenJDK 11 tarball
RUN wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.28%2B6/OpenJDK11U-jdk_x64_linux_hotspot_11.0.28_6.tar.gz || { echo "Failed to download OpenJDK tarball"; exit 1; }

# Extract the tarball
RUN tar -xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.28_6.tar.gz || { echo "Failed to extract tarball"; exit 1; }

# Create JVM directory and move JDK
RUN mkdir -p /usr/lib/jvm && \
    mv jdk-11.0.28+6 /usr/lib/jvm/java-11-openjdk-amd64 || { echo "Failed to move JDK to /usr/lib/jvm/java-11-openjdk-amd64"; exit 1; }

# Clean up tarball
RUN rm OpenJDK11U-jdk_x64_linux_hotspot_11.0.28_6.tar.gz || { echo "Failed to remove tarball"; exit 1; }

# Set permissions and ownership
RUN chmod -R a+rx /usr/lib/jvm/java-11-openjdk-amd64 && \
    chown -R airflow:airflow /usr/lib/jvm/java-11-openjdk-amd64 || { echo "Failed to set permissions or ownership"; exit 1; }

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:/usr/lib/jvm/java-11-openjdk-amd64/bin

# Switch back to airflow user for runtime security
USER airflow