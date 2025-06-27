

### Add ssh secret
```
kubectl create secret generic data-ssh-key --from-file=id_rsa=/home/research/.ssh/id_rsa
```

docker save mynginx > /tmp/myimage.tar

#### Import to microk8s
microk8s.ctr -n k8s.io image import myimage.tar


#### Import to containerd
ctr -n=k8s.io images import /tmp/myimage.tar
