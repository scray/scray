

## Build job images

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
sudo docker run -p 7892:8888 -p 7851:7850 --name ki2 --runtime=nvidia --gpus all -e JOB_NAME=ki1_tensorflow_pytorch --name ki1_tensorflow_pytorch -v /home/research/workspaces/ml-integration:/tf2 -v /home/research/workspaces/huggingface/cache/huggingface:/root/.cache/huggingface -v ~/.ssh:/root/.ssh:ro scray-jupyter_tensorflow_pytorch-gpu:0.1.1
```

### huggingface-transformers-pytorch-deepspeed-latest-gpu
```
docker build -t huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2 -f ./docker-image-descriptions/huggingface-transformers-pytorch-deepspeed-latest-gpu/Dockerfile .
docker run --runtime=nvidia --gpus all --ipc=host --ulimit memlock=-1 --ulimit stack=67108864  -e JOB_NAME=deepspeed1 --name deepspeed1 -v /mnt/ssd2/huggingface/cache/huggingface:/root/.cache/huggingface -v /mnt/ssd2/ml-models:/root/ml-models -v ~/.ssh:/root/.ssh:ro huggingface-transformers-pytorch-deepspeed-latest-gpu:0.1.1

```

### huggingface-transformers-pytorch-deepspeed-latest-gpu-dep
```
docker build -t huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2 -f ./docker-image-descriptions/huggingface-transformers-pytorch-deepspeed-latest-gpu-dep/Dockerfile .

docker run --runtime=nvidia --gpus all --ipc=host --ulimit memlock=-1 --ulimit stack=67108864  -e JOB_NAME=deepdep1 --name deepdep1 -v /mnt/ssd2/huggingface/cache/huggingface:/root/.cache/huggingface -v /mnt/ssd2/ml-models:/root/ml-models -v ~/.ssh:/root/.ssh:ro huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.1

```
