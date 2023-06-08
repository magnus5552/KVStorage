# KVStorage
<h3>Key-value storage</h3>
<h1>Usage</h1>
Start server:

```shell
> python KVserver.py
```
Start client:

```shell
> python KVstorage.py
```

Subscribe client to server:

```shell
> python KVstorage.py subscribe <server host> <server port>
```

Write data to storage:

```shell
> python KVstorage.py write <key> <value>
```

Get data from storage

```shell
> python KVstorage.py get <key>
```

Stop client

```shell
> python KVstorage.py stop
```