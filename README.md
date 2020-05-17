# Distributed KV Store

### ABD
To run abd protocol, first move to the abd directory.
```bash
cd abd/
```
Then compile the code.
```bash
make
```
The config files and executable will be under directory /build.

### Blocking
To run block protocol, first move to the block directory.
```bash
cd block/
```
Then compile the code.
```bash
make
```
The config files and executable will be under directory /build.


### client commands
Read
```bash
./abd_client read [key]
./block_client read [key]
```

Write
```bash
./abd_client write [key] [value]
./block_client write [key] [value]
```
