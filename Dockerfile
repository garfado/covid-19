FROM apache/airflow:2.7.3

USER root
RUN apt-get update && apt-get install -y bash openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN ln -sf /bin/bash /usr/bin/bash && which bash && echo "Bash instalado com sucesso!"
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install pyspark==3.1.1

# Comando padrão para iniciar o Airflow como uma instância completa
CMD ["airflow", "standalone"]

