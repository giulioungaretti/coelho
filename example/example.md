load data inot rabbit mq
```sh
❯ source set_env.sh && zcat -r ~/ShopGun/data/shoppingitem/add | rkMessage  | go run $GOPATH/src/github.com/giulioungaretti/coelho/example/pub.go
```

