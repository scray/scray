FROM python:3.10.12

ARG GRADIO_SERVER_PORT=7860
ENV GRADIO_SERVER_PORT=${GRADIO_SERVER_PORT}

RUN apt-get update && apt-get install -y jq 

WORKDIR /workspace

COPY scripts/process-job.sh /opt/scray-sync/

ADD example-app/requirements.txt example-app/app.py /workspace/

RUN pip install -r /workspace/requirements.txt

CMD ["bash", "-c", "/opt/scray-sync/process-job.sh"]
