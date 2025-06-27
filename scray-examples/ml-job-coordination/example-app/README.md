### Start a in /see/os environement


```
export SCRAY_DATA_INTEGRATION_USER=stefan
export SCRAY_DATA_INTEGRATION_HOST=127.0.0.1
export SCRAY_SYNC_API_URL=http://127.0.0.1:8082
```

```
./run-on-ki1.sh run\
  --take-jobname-literally true\
  --job-name gradio-example-app\
  --processing-env http://scray.org/ai/app/env/see/os/k8s/k8s-deployment\
  --notebook-name app.py\
  --docker-image  scrayorg/python:0.1.3
```