FROM hyperledger/fabric-tools:2.2.1

RUN apk add curl zip
RUN mkdir /mnt/peer-conf && mkdir /mnt/scripts/

COPY conf /tmp/conf

CMD cp -r /tmp/conf /mnt/peer-conf/ && cd /mnt/peer-conf/conf/ && ./run.sh -d $HOSTNAME -o $ORG_NAME -t $NODE_TYPE -s $DATA_SHARE
