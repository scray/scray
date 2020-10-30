FROM hyperledger/fabric-tools:2.2.1

RUN apk add curl zip
RUN mkdir /mnt/peer-conf

COPY conf /tmp/conf

CMD cp -r /tmp/conf /mnt/peer-conf/ && cd /mnt/peer-conf/conf/ && ./run.sh 
