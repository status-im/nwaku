
## Pre-requisite
libwaku.so is needed to be compiled and present in build folder. To create it:
```code
make POSTGRES=1 libwaku -j4
```

## Compilation

From nwaku root folder, do

```code
go build -o waku-go examples/golang/waku.go
```

## Run
From the nwaku root folder:


```code
export LD_LIBRARY_PATH=build
```

```code
./waku-go
```
