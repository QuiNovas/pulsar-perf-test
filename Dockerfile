FROM python:3.7-slim
LABEL maintainer="Mathew Moon <mmoon@quinovas.com>"


RUN apt update && \
    apt install -y build-essential && \
    pip install pulsar-client==2.3.1 && \
    apt remove -y --purge build-essential && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY ./PulsarTest.py /app/PulsarTest.py
COPY ./main.py /app/main.py

RUN chmod +x /app/*

WORKDIR /app

CMD /app/main.py
