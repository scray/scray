FROM python:3.10.12

ARG GRADIO_SERVER_PORT=7860
ENV GRADIO_SERVER_PORT=${GRADIO_SERVER_PORT}

WORKDIR /workspace

ADD requirements.txt app.py /workspace/

RUN pip install -r /workspace/requirements.txt

CMD ["python3", "/workspace/app.py"]