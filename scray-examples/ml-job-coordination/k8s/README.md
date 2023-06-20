

### Add ssh secret
```
kubectl create secret generic data-ssh-key --from-file=id_rsa=/home/research/.ssh/id_rsa
```

docker save mynginx > myimage.tar
microk8s.ctr -n k8s.io image import myimage.tar
