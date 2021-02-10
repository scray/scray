BASE_PATH=$PWD

yq() {
  $BASE_PATH/bin/yq $1 $2 $3 $4 $5
}

# Check if yq exists
checkYqVersion() {
  dowloadYqBin
}

dowloadYqBin() {
  if [[ ! -f "./bin/yq" ]]
  then
    echo "yq does not exists"
    echo "download linux_amd64 yq binary"

    mkdir bin
    curl -L https://github.com/mikefarah/yq/releases/download/3.4.1/yq_linux_amd64 -o ./bin/yq
    chmod u+x ./bin/yq
  fi
}

yq -V
