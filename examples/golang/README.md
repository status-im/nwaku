
## Pre-requisite
libwaku.so is needed to be compiled and present in build folder. To create it:

1. Run only the first time and after changing the current commit
```code
make update
```
2. Run the next every time you want to compile libwaku
```code
make POSTGRES=1 libwaku -j4
```

## Compilation

From the nwaku root folder:

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
