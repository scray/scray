
```
docker build -t gradio-app .
docker run -p 7860:7860 gradio-app
```

```
./run-on-ki1.sh run\ 
  --job-name timestamp-example\
  --notebook-name timestamp-example.ipynb\
  --processing-env http://scray.org/ai/app/env/see/ki1-k8s
```

```
./run-on-ki1.sh run\
  --take-jobname-literally true\
  --job-name gradio-example-app\ 
  --processing-env http://scray.org/ai/app/env/see/os\
  --notebook-name app.py\
  --docker-image  scray/python:0.1.2
```