# General

Call forwarder is a US/CA Number Portability server with custom in-memory database engine.

The main feature is high-speed mapping 10-digit phone number into 10-digit routing number.

In addition, it support inverse operation of listing all numbers that maps into specified routing number prefix.

# Installation

- Compile proxygen using `proxygen/proxygen/build.sh` script
- Install `proxygen/proxygen/_build/deps/{bin,lib,include}` into `callfwd/deps/{bin,lib,include}`
- Install `proxygen/proxygen/_build/{bin,lib,include}` into `callfwd/deps/{bin,lib,include}`
- Build `callfwd` binary using `cmake`
- Install `callfwd` binary and `conf/` to the target machine (check `build-centos.sh` as example)
- Read and modify `/etc/callfwd.{flags,env}` as needed
- Start service and autoupdate timers using `sudo systemctl start callfwd.service`

# Notes on CentOS 7
- Use devtoolset-9 and tbb 2019, set environment variables as in Dockerfile
- When building proxygen, compile gflags glog and boost 1.75 from sources and install them into `_build/deps`
- Folly's check for SIMD support is broken, modify its `CMakeLists.txt` to always use SIMD
``` diff
-  if (${IS_X86_64_ARCH} STREQUAL "-1")
+  #if (${IS_X86_64_ARCH} STREQUAL "-1")
+  if (FALSE)
```
- Modify `proxygen/build.sh` to use static boost libraries (copy line from fuzzing section)
``` diff
@@ -208,9 +208,9 @@ function setup_folly() {
   MAYBE_USE_STATIC_DEPS=""
   MAYBE_USE_STATIC_BOOST=""
   MAYBE_BUILD_SHARED_LIBS=""
+    MAYBE_USE_STATIC_BOOST="-DBOOST_LINK_STATIC=ON"
```
- After copying `proxygen` artifacts into `deps` modify `proxygen-config.cmake` and remove `find_depedency(mvfst)` line

# Operation

The main binary is expected to run through `systemd` service unit.
The service unit is paired with unix datagram socket used for passing commands into daemon without restarting it.
You should use `callfwdctl` script communicate with daemon. It supports the following subcommands:
- `reload` - reload US/CA phone mapping from `.txt` or `.tar.gz` file
- `verify` - check if loaded mapping in memory matches file on disk
- `dump` - write loaded mapping from memory to disk
- `acl` - reload ACL rules from file
- `status` - show information about loaded database

After starting, `callfwd` will listen HTTP and SIP ports and respond with `503` until both US and CA mappings are loaded.

# Diagnostics

The following commands should be useful to troubeshoot `callfwd` behaviour:
- `sudo systemctl status callfwd.{service,socket} callfwd-acl.{service,timer} callfwd-reload.{service,timer}`
- `sudo journalctl -xfu callfwd.service`
- `callfwdctl status`

# Logging

All request are logged to file specified by `--access_log` flag. Both HTTP and SIP requests logged into the same stream with Apache-style format:
```
46.242.10.41:2623 - - [21/Jan/2021:09:44:28 +0000] "INVITE sip:+12012000001@204.9.202.153:5060;user=phone" 401 0
46.242.10.41:2582 - - [21/Jan/2021:09:44:53 +0000] "POST /target" 200 210
```

You can send `HUP` signal to the daemon after rotation to reopen stream:
```
sudo systemctl kill --signal HUP callfwd.service
```

# HTTP API

`callfwd` registers two HTTP endpoints:
- `/target` (`GET`, `POST`) - map a batch of phone numbers into routing numbers
- `/reverse` (`GET`) - map a batch of routing prefixes into phone numbers

Both requests supports two output formats: `csv` and `json`. By default `csv` format is used.
To use `json` you need to ask it explicitly using `Accept` header.
For example: `curl -H "Accept: application/json"` or `http --json`.

Note that maximum length of a `POST` body is controlled by `--max_query_length` flag.

## Examples
``` http
GET /target?phone[]=9899999992&phone[]=9899999995 HTTP/1.1
Accept: */*

HTTP/1.1 200 OK
Content-Type: text/plain

9899999992,9895010081
9899999995,9894590000

POST /target HTTP/1.1
Content-Type: application/x-www-form-urlencoded
Accept: */*

phone[]=9899999992&phone[]=9899999995

HTTP/1.1 200 OK
Content-Type: text/plain

9899999992,9895010081
9899999995,9894590000

GET /reverse?prefix[]=9895&prefix[]=9894 HTTP/1.1
Accept: */*

HTTP/1.1 200 OK
Content-Type: text/plain

9892001376,9895010012
9892001559,9895010012
9892001846,9895010012
...
9898376038,9894929997
9898376039,9894929997
9898390500,9894929997

GET /target?phone[]=9899999992&phone[]=9899999995 HTTP/1.1
Accept: application/json, */*;q=0.5

HTTP/1.1 200 OK
Content-Type: application/json

[
  {"pn": "9899999992", "rn": "9895010081"},
  {"pn": "9899999995", "rn": "9894590000"},
]
```


# SIP API

`callfwd` registers two SIP endpoints:
- `INVITE` - return `302 Moved` and add `rn` parameter into `Contact` field 
- `OPTIONS` - always returns `200`

Note that if requested phone not found in DB `rn` parameter will contain a copy of original number.
If you wan't to follow RFC4694 and skip `rn` parameter in that case use `--rfc4694` flag.

Access to the SIP handler is limited by ACL rules:
- If client IP is not listed in ACL `401 Unauthorized` returned
- If client exceeds maximum number of calls per second `429 Too Many Requests` returned

All phone numbers must be 10-digit US or Canada numbers.
It's allowed to use `1` and `+1` prefixes, `-` delimiters and `()` brackets.
All other numbers are treated as international and will not be checked in DB.

## Example
```
2020/12/23 12:09:11.582212 204.9.202.155:4319 -> 204.9.202.153:5060
INVITE sip:+12018660805@204.9.202.153:5060;user=phone SIP/2.0
Via: SIP/2.0/UDP 204.9.202.155:4319;branch=z9h64bK1953406209
From: <sip:18666654386@204.9.202.155:4319;user=phone>;tag=339948729
To: <sip:12018660805@204.9.202.153:5060;user=phone>
Call-ID: s0-9bca09cc-0fd4fc38-7f21bd45-2d08fdc6
CSeq: 2311677411 INVITE
Max-Forwards: 70
Contact: <sip:18666654386@204.9.202.155:4319;transport=udp>
Content-Type: application/sdp
Content-Length: 0

2020/12/23 12:09:11.582339 204.9.202.153:5060 -> 204.9.202.155:4319
SIP/2.0 302 Moved Temporarily
Via: SIP/2.0/UDP 204.9.202.155:4319;branch=z9h64bK1953406209
From: <sip:18666654386@204.9.202.155:4319;user=phone>;tag=339948729
To: <sip:12018660805@204.9.202.153:5060;user=phone>;tag=13bd22f6
Call-ID: s0-9bca09cc-0fd4fc38-7f21bd45-2d08fdc6
CSeq: 2311677411 INVITE
Contact: <sip:+12018660805;rn=+12018209999;npdi@204.9.202.153:5060>
Location-Info: N
Content-Length: 0
```
