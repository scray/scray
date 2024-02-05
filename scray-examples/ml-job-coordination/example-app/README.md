### Start a in /see/os environement

```
./run-on-ki1.sh run\
  --take-jobname-literally true\
  --job-name gradio-example-app\ 
  --processing-env http://scray.org/ai/app/env/see/os\
  --notebook-name app.py\
  --docker-image  scray/gradio-example:0.1.3
```