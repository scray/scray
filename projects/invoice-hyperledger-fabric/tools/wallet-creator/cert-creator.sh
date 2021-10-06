#!/bin/bash

apk add curl
apk add openssl

export MSYS_NO_PATHCONV=1 # Dont expand / as path

CA_CERT="../ca.org1.example.com-cert.pem"
CA_KEY="../priv_sk"
NEW_CERT_COMMON_NAME="user1"
ORGANIZATIONAL_UNIT="admin"
CREATE_WALLET=false
WALLET_CREATOR_JAR_PATH=./target
MSP_ID="org1MSP"
SHARED_FS_HOST=127.0.0.1

SHARED_FS_USER=scray
SHARED_FS_PW=scray

USR_CERT=""
USR_KEY=""


createKeyAndCsr() {
  echo "Create csr  for $NEW_CERT_COMMON_NAME"
	NEW_CERT_TARGET_PATH=crt_target/$NEW_CERT_COMMON_NAME
	mkdir -p $NEW_CERT_TARGET_PATH
	openssl ecparam -name prime256v1 -genkey -noout -out $NEW_CERT_TARGET_PATH/key.pem
	openssl req -new -sha256 -key $NEW_CERT_TARGET_PATH/key.pem -out $NEW_CERT_TARGET_PATH/user.csr  -subj  "/CN=$NEW_CERT_COMMON_NAME /O=kubernetes.research.dev.seeburger.de /OU=$ORGANIZATIONAL_UNIT"
}

pushCsr() {
  NEW_CERT_COMMON_NAME=$1
  SHARED_FS_HOST=$2

  echo $SHARED_FS_HOST
  curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/csrs_to_sign/
  curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/csrs_to_sign/$NEW_CERT_COMMON_NAME
  curl --user $SHARED_FS_USER:$SHARED_FS_PW -T crt_target/$NEW_CERT_COMMON_NAME/user.csr http://$SHARED_FS_HOST/csrs_to_sign/$NEW_CERT_COMMON_NAME/user.csr
}

pullCsr() {
  curl --user $SHARED_FS_USER:$SHARED_FS_PW http://$SHARED_FS_HOST/csrs_to_sign/$NEW_CERT_COMMON_NAME/user.csr > $NEW_CERT_COMMON_NAME.csr
}

signCsr() {
	NEW_CERT_TARGET_PATH=crt_target/$NEW_CERT_COMMON_NAME
	openssl x509 -req -days 360 -in $NEW_CERT_COMMON_NAME.csr -CA $CA_CERT -CAkey $CA_KEY -CAcreateserial -out $NEW_CERT_COMMON_NAME.crt
}

pushCrt() {
  NEW_CERT_COMMON_NAME=$1
  SHARED_FS_HOST=$2

  curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/signed_certs/
  curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/signed_certs/$NEW_CERT_COMMON_NAME
  curl --user $SHARED_FS_USER:$SHARED_FS_PW -T $NEW_CERT_COMMON_NAME.crt http://$SHARED_FS_HOST/signed_certs/$NEW_CERT_COMMON_NAME/$NEW_CERT_COMMON_NAME.crt
}

pullCrt() {
  mkdir  -p crt_target/$NEW_CERT_COMMON_NAME
  curl --user $SHARED_FS_USER:$SHARED_FS_PW http://$SHARED_FS_HOST/signed_certs/$NEW_CERT_COMMON_NAME/$NEW_CERT_COMMON_NAME.crt > crt_target/$NEW_CERT_COMMON_NAME/user.crt
}

compileWalletCreator() {
	mvn clean install
}

createWallet() {
	echo $WALLET_CREATOR_JAR_PATH
	if [ $WALLET_CREATOR_JAR_PATH = ./target ]
	then
		compileWalletCreator
	fi
	java -jar $WALLET_CREATOR_JAR_PATH/wallet-creator-0.0.1-SNAPSHOT-jar-with-dependencies.jar crt_target/$NEW_CERT_COMMON_NAME/key.pem crt_target/$NEW_CERT_COMMON_NAME/user.crt $NEW_CERT_COMMON_NAME $MSP_ID
}

usage()
{
    echo "Unknown parameter" $1
    echo "usage: Create wallet [[[-cn | --common-name ] [-d]] | [-h]]"
}

readParameters()
{
  while [ "$1" != "" ]; do
      case $1 in
          -c | --cacert )   shift
                            CA_CERT=$1
                                  ;;
          -k | --cakey )   		shift
                    CA_KEY=$1
                                  ;;
          -cn | --common-name)	shift
                  NEW_CERT_COMMON_NAME=$1
                            ;;
          --mspId)	shift
                  MSP_ID=$1
                             ;;
        -o | --organizational-unit)  shift
              ORGANIZATIONAL_UNIT=$1
                                   ;;
          -w | --create-wallet) shift
            CREATE_WALLET=$1
                           ;;
        -j | --wallet-creator-lib-path) shift
            WALLET_CREATOR_JAR_PATH=$1
          ;;
        --shared-fs-host) shift
          SHARED_FS_HOST=$1
          ;;
          -h | --help )
                                  usage $1
                                  exit
                                  ;;
          * )
                                  usage $1
                                  exit 1
      esac
      shift
  done
}



COMMAND=$1
shift
readParameters $@

case $COMMAND in
  create_csr)
    echo  "create_csr"
    readParameters $@
    createKeyAndCsr
    ;;
  push_csr)
    echo  "push_csr"
    pushCsr "$NEW_CERT_COMMON_NAME" "$SHARED_FS_HOST"
    ;;
  pull_csr)
    echo  "pull_csr"
    pullCsr
    ;;
  sign_csr)
    echo  "sign_csr"
    signCsr "$NEW_CERT_COMMON_NAME"
    ;;
  push_crt)
    echo  "publish_crt"
    pushCrt "$NEW_CERT_COMMON_NAME" "$SHARED_FS_HOST"
    ;;
  pull_signed_crt)
    echo  "pull_signed_crt"
    pullCrt
    ;;
  create_wallet)
    echo ""
    createWallet
    ;;
esac




echo "Configuration"
echo "  CA cert path:   ${CA_CERT} "
echo "  CA key path:    ${CA_KEY}  "
echo "  CN of new cert: ${NEW_CERT_COMMON_NAME} "
echo "  Organisation:   ${ORG} "

# createKeyAndCsr
# signCert
# createWallet

