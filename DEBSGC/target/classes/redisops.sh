#!/bin/bash

/home/abhinav/Desktop/redis/redis-stable/src/redis-cli shutdown
nohup /home/abhinav/Desktop/redis/redis-stable/src/redis-server &
/home/abhinav/Desktop/redis/redis-stable/src/redis-cli FLUSHALL
