
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