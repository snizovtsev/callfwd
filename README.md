Example
========

Running:

``` sh
./callfwd ../20201218-lrn-bigdata.tail.txt
```

Map phone mapping:

``` http
GET /target?phone%5B%5D=9899999992&phone%5B%5D=9899999995 HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Host: localhost:11000
User-Agent: HTTPie/2.3.0

HTTP/1.1 200 OK
Connection: keep-alive
Content-Type: text/plain
Date: Mon, 28 Dec 2020 19:18:18 GMT
Transfer-Encoding: chunked

9895010081,9894590000
```

Get reverse mapping:

``` http
GET /reverse?prefix%5B%5D=9895 HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Host: localhost:11000
User-Agent: HTTPie/2.3.0



HTTP/1.1 200 OK
Connection: keep-alive
Content-Type: text/plain
Date: Mon, 28 Dec 2020 19:37:22 GMT
Transfer-Encoding: chunked

9899999990,9899999991,9899999992,9899999997,9899999998,9899999999
```
