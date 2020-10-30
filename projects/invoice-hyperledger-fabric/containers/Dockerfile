FROM ubuntu

RUN apt-get update
RUN apt-get --assume-yes  install curl git docker-compose vim screen

CMD cd /mnt/hl-fabric-conf/ && curl -sSL https://bit.ly/2ysbOFE | bash -s && cd /mnt/hl-fabric-conf/fabric-samples/test-network && ./network.sh up
