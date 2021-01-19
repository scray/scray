#!/bin/bash

CA_CERT="../ca.org1.example.com-cert.pem"
CA_KEY="../priv_sk"
NEW_CERT_COMMON_NAME="user1"
ORGANIZATIONAL_UNIT="admin"
CREATE_WALLET=false
USR_CERT=""
USR_KEY=""


createKeyAndCsr() {
	NEW_CERT_TARGET_PATH=crt_target/$NEW_CERT_COMMON_NAME
	mkdir -p $NEW_CERT_TARGET_PATH
	openssl ecparam -name prime256v1 -genkey -noout -out $NEW_CERT_TARGET_PATH/key.pem
	openssl req -new -sha256 -key $NEW_CERT_TARGET_PATH/key.pem -out user.csr -subj "/CN=$NEW_CERT_COMMON_NAME /O=org1.example.com /OU=$ORGANIZATIONAL_UNIT"
}

signCert() {
	NEW_CERT_TARGET_PATH=crt_target/$NEW_CERT_COMMON_NAME
	openssl x509 -req -days 360 -in user.csr -CA $CA_CERT -CAkey $CA_KEY -CAcreateserial -out $NEW_CERT_TARGET_PATH/user.crt
}

compileWalletCreator() {
	mvn clean install
}

createWallet() {
	if [ $CREATE_WALLET = true ]
	then
		compileWalletCreator
		java -jar target/wallet-creator-0.0.1-SNAPSHOT-jar-with-dependencies.jar crt_target/$NEW_CERT_COMMON_NAME/key.pem crt_target/$NEW_CERT_COMMON_NAME/user.crt $NEW_CERT_COMMON_NAME
	fi
}

usage()
{
    echo "usage: Create wallet [[[-o ] [-d]] | [-h]]"
}


while [ "$1" != "" ]; do
    case $1 in
        -c | --cacert )   shift
                      	  CA_CERT=$1
                                ;;
        -k | --cakey )   		shift
	       				  CA_KEY=$1	
                                ;;
        -n | --new-user-crt)	shift
	       			  NEW_CERT_COMMON_NAME=$1
			  ;;
	-o | --organizational-unit)  shift
					  ORGANIZATIONAL_UNIT=$1
                                ;;
        -w| --create-wallet)  shift
					CREATE_WALLET=true
				;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

echo "Configuration"
echo "  CA cert path:   ${CA_CERT} "
echo "  CA key path:    ${CA_KEY}  "
echo "  CN of new cert: ${NEW_CERT_COMMON_NAME} "

createKeyAndCsr
signCert
createWallet

