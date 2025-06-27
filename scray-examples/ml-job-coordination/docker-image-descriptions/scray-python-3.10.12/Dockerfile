FROM python:3.10.12

RUN apt-get update && apt-get install -y jq 

WORKDIR /workspace

COPY scripts/process-job.sh /opt/scray-sync/

ADD docker-image-descriptions/scray-python-3.10.12/requirements.txt /workspace/

RUN pip install --upgrade pip ipython ipykernel
RUN ipython kernel install --name "python3" --user

RUN pip install -r /workspace/requirements.txt

CMD ["bash", "-c", "/opt/scray-sync/process-job.sh"]
