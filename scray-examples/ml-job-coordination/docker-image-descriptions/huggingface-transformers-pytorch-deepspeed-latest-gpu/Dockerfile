FROM huggingface/transformers-pytorch-deepspeed-latest-gpu:latest

RUN apt-get update && apt-get -y install curl openssh-server jq

COPY scripts/process-job.sh /opt/scray-sync/

RUN pip install papermill

CMD ["bash", "-c", "/opt/scray-sync/process-job.sh"]