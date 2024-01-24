
## Examples
    * [Example job](example-jobs/README.md)
    * [Example app](example-app/README.mdd)

## Prepared images

### jupyter_spark
```
docker build -t scray-jupyter-spark:0.1.1 -f .\docker-image-descriptions\jupyter_all-spark-notebook\Dockerfile .
```

### jupyter_tensorflow_latest-gpu
```
docker build -t scray-jupyter_tensorflow-gpu:0.1.1 -f ./docker-image-descriptions/jupyter_tensorflow_latest-gpu/Dockerfile .
docker run --runtime=nvidia --gpus all -e JOB_NAME=timestamp-example --name timestamp-example -v ~/.ssh:/root/.ssh:ro scray-jupyter_tensorflow-gpu:0.1.1
```

### jupyter_tensorflow_pytorch_latest-gpu
```
docker build -t scray-jupyter_tensorflow_pytorch-gpu:0.1.1 -f ./docker-image-descriptions/jupyter_tensorflow_pytorch_latest-gpu/Dockerfile .
```

### Python image

```
docker build -t scray/python:0.1.2 -f ./example-app/Dockerfile .
```

```
docker build -t scray/python:0.1.3 -f ./docker-image-descriptions/scray-python-3.10.12/Dockerfile .
```

### huggingface-transformers-pytorch-deepspeed-latest-gpu
```
docker build -t huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2 -f ./docker-image-descriptions/huggingface-transformers-pytorch-deepspeed-latest-gpu/Dockerfile .
docker run --runtime=nvidia --gpus all --ipc=host --ulimit memlock=-1 --ulimit stack=67108864  -e JOB_NAME=deepspeed1 --name deepspeed1 -v /mnt/ssd2/huggingface/cache/huggingface:/root/.cache/huggingface -v /mnt/ssd2/ml-models:/root/ml-models -v ~/.ssh:/root/.ssh:ro huggingface-transformers-pytorch-deepspeed-latest-gpu:0.1.2

```

### huggingface-transformers-pytorch-deepspeed-latest-gpu-dep
```
docker build -t huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2 -f ./docker-image-descriptions/huggingface-transformers-pytorch-deepspeed-latest-gpu-dep/Dockerfile .

docker run --runtime=nvidia --gpus all --ipc=host --ulimit memlock=-1 --ulimit stack=67108864  -e JOB_NAME=deepdep1 --name deepdep1 -v /mnt/ssd2/huggingface/cache/huggingface:/root/.cache/huggingface -v /mnt/ssd2/ml-models:/root/ml-models -v ~/.ssh:/root/.ssh:ro huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2

```

### seamless_m4t
```
docker build -t seamless_m4t:0.1.2 -f ./docker-image-descriptions/seamless_m4t/Dockerfile .
docker save seamless_m4t:0.1.2 > /tmp/q1.tar
sudo ctr -n=k8s.io images import /tmp/q1.tar
```

## Commonly used external resources
### SSH credentials to login to integration server

```
kubectl create secret generic data-ssh-key --from-file=id_rsa=/home/ubuntu/.ssh/id_rsa
```



