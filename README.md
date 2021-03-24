# bittorent-client

Requirements: python version >= 3.6

# Setup Python3.6 on Ubuntu 16.04
```
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.6
```
# Sample run
```
# Launch server
python3.6 ./server.py --port 10000

# Launch peer 1
python3.6 ./peer.py  --data-dir $(pwd)/files/p1 --peer-id 1 --server-ip localhost --server-port 10000

```
