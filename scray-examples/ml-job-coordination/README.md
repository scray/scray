

## Build job images

### jupyter_spark
```
docker build -t scray-jupyter-spark:0.1.1 -f .\docker-image-descriptions\jupyter_all-spark-notebook\Dockerfile .
```

### jupyter_tensorflow_latest-gpu
```
docker build -t scray-jupyter_tensorflow-gpu:0.1.1 -f ./docker-image-descriptions/jupyter_tensorflow_latest-gpu/Dockerfile .
docker run -p 8891:8888 --runtime=nvidia  --gpus all -e JOB_NAME=job4711 -v ~/.ssh:/root/.ssh:ro scray-jupyter_tensorflow-gpu:0.1.1
```
