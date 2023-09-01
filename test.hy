(eval-when-compile
  (require
    hyrule :readers * *
    asyncrule *
    structlib *))

(import
  structlib *
  socket
  enum [IntEnum])

(defn http-headers-pack [headers]
  (doto (lfor #(k v) (headers.items) (.format "{}: {}" k v))
        (.append "")))

(defn http-headers-unpack [headers]
  (.pop headers)
  (dfor header headers
        :setv #(k v) (.split header ":" 1)
        (.strip k) (.strip v)))

(defstruct HTTPFirstLine
  [[line firstline
    :sep b"\r\n"
    :from (.join " " it)
    :to (.split it)]])

(defstruct HTTPHeaders
  [[line headers
    :sep b"\r\n"
    :repeat-until (not it)
    :from (http-headers-pack it)
    :to (http-headers-unpack it)]])

(defstruct HTTPReq
  [[struct [[meth path ver]] :struct (async-name HTTPFirstLine)]
   [struct [headers] :struct (async-name HTTPHeaders)]])

(defstruct HTTPResp
  [[struct [[ver status reason]] :struct (async-name HTTPFirstLine)]
   [struct [headers] :struct (async-name HTTPHeaders)]])

(defstruct UDP
  [[int [sport dport] :len 2 :repeat 2]
   [int plen :len 2]
   [int cksum :len 2]])

(defstruct TCP
  [[int [sport dport] :len 2 :repeat 2]
   [int [seq ack] :len 4 :repeat 2]
   [bits [hlen res U A P R S F] :lens [4 6 1 1 1 1 1 1]]
   [int win :len 2]
   [int cksum :len 2]
   [int uptr :len 2]])

(defstruct IPv4Addr
  [[bytes addr
    :len 4
    :from (socket.inet-pton socket.AF-INET it)
    :to (socket.inet-ntop socket.AF-INET it)]])

(defstruct IPv6Addr
  [[bytes addr
    :len 16
    :from (socket.inet-pton socket.AF-INET6 it)
    :to (socket.inet-ntop socket.AF-INET6 it)]])

(defstruct IPv4
  [[bits [ver hlen] :lens [4 4]]
   [int tos :len 1]
   [int tlen :len 2]
   [int id :len 2]
   [bits [flag offset] :lens [3 13]]
   [int ttl :len 1]
   [int proto :len 1]
   [int cksum :len 2]
   [struct [[src] [dst]] :struct (async-name IPv4Addr) :repeat 2]])

(defstruct IPv6
  [[bits [ver tc fl] :lens [4 8 20]]
   [int plen :len 2]
   [int nh :len 1]
   [int hlim :len 1]
   [struct [[src] [dst]] :struct (async-name IPv6Addr) :repeat 2]])

(defclass Socks5Atype [IntEnum]
  (setv DN 3 V4 1 V6 4))

(defstruct Socks5DNAddr
  [[varlen addr
    :len 1
    :from (.encode it)
    :to (.decode it)]])

(defstruct Socks5Addr
  [[int atype :len 1]
   [struct [host]
    :struct (ecase atype
                   Socks5Atype.DN (async-name Socks5DNAddr)
                   Socks5Atype.V4 (async-name IPv4Addr)
                   Socks5Atype.V6 (async-name IPv6Addr))]
   [int port :len 2]])

(defstruct Socks5AuthReq
  [[int ver :len 1 :to-validate (= it 5)]
   [varlen meths :len 1 :to-validate (in 0 it)]])

(defstruct Socks5AuthRep
  [[int ver :len 1 :to-validate (= it 5)]
   [int meth :len 1 :to-validate (= it 0)]])

(defstruct Socks5Req
  [[int ver :len 1 :to-validate (= it 5)]
   [int cmd :len 1 :to-validate (= it 1)]
   [int rsv :len 1 :to-validate (= it 0)]
   [struct [atype host port] :struct (async-name Socks5Addr)]])

(defstruct Socks5Rep
  [[int ver :len 1 :to-validate (= it 5)]
   [int rep :len 1 :to-validate (= it 0)]
   [int rsv :len 1 :to-validate (= it 0)]])

(defstruct TrojanReq
  [[line auth :sep b"\r\n"]
   [int cmd :len 1 :to-validate (= it 1)]
   [struct [atype host port] :struct (async-name Socks5Addr)]
   [line empty :sep b"\r\n" :to-validate (not it)]])
