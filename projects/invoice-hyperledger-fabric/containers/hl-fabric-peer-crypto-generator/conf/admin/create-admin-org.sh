#!/bin/bash

# Add cryptongen binary to path in develop environment
export PATH=~/git/fabric-samples/bin:$PATH

echo "Create AdminOrg MSP"
cryptogen generate --config=crypto.yaml --output=./organizations
