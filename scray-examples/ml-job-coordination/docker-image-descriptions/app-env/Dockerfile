FROM python:3.10.12

ARG GRADIO_SERVER_PORT=7860
ENV GRADIO_SERVER_PORT=${GRADIO_SERVER_PORT}

RUN apt-get update && apt-get -y install jq


WORKDIR /opt/scray-sync/

COPY scripts/process-job.sh /opt/scray-sync/


CMD ["bash", "-c", "/opt/scray-sync/process-job.sh"]