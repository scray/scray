FROM jupyter/all-spark-notebook:2023-04-24

USER root

RUN apt-get update && apt-get -y install curl openssh-server jq

USER jovyan
COPY scripts/process-job.sh /opt/scray-sync/

RUN pip install papermill

CMD ["bash", "-c", "/opt/scray-sync/process-job.sh; tail -f /dev/null"]