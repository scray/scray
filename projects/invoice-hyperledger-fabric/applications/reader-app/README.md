```
cd ~/git/fabric-samples/
export PATH=~/git/fabric-samples/bin:$PATH
```

### Start network
```
./network.sh up
./network.sh createChannel
./network.sh deployCC -ccn basic -ccp  ../asset-transfer-basic/chaincode-java -ccl java
```

### Stop network

```
./network.sh down
```


```
./network.sh up createChannel -c mychannel -ca
./network.sh deployCC -ccn basic -ccp ../asset-transfer-basic/chaincode-java/  -ccl java

```