FROM tensorflow/tensorflow:2.11.1-gpu-jupyter

RUN apt-get update && apt-get -y install curl openssh-server jq

COPY scripts/process-job.sh /opt/scray-sync/

RUN pip install papermill

CMD ["bash", "-c", "/opt/scray-sync/process-job.sh"]