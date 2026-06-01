#!/bin/bash

cd ..
cd crawler
docker build -t crawler_image:1.0.0 .

cd ..
cd docker_deployment
docker compose up -d --build
