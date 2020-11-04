FROM hyperledger/fabric-tools:2.2.1

RUN apk add curl zip
RUN mkdir /mnt/peer-conf && mkdir /mnt/scripts/

COPY conf /tmp/conf
COPY scripts /tmp/scripts

CMD cp -r /tmp/scripts /mnt/scripts/ && cp -r /tmp/conf /mnt/peer-conf/ && cd /mnt/peer-conf/conf/ && ./run.sh 
