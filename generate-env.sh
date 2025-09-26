#!/bin/bash
# generate-env.sh

HOST_IP=$(ip route get 1 | awk '{print $(NF-2); exit}')
echo "HOST_IP=$HOST_IP" > .env