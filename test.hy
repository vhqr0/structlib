(eval-when-compile
  (require
    hyrule :readers * *
    asyncrule *
    structlib *))

(import
  structlib *
  socket
  enum [IntEnum])

(defn http-pack [headers]
  (doto (lfor #(k v) (headers.items) (.format "{}: {}" k v))
        (.append "")))

(defn http-unpack [headers]
  (.pop headers)
  (dfor header headers
        :setv #(k v) (.split header ":" 1)
        (.strip k) (.strip v)))

(defstruct HTTPHeaders
  [[line headers
    :sep b"\r\n"
    :repeat-until (not it)
    :from (http-pack it)
    :to (http-unpack it)]])

(defstruct HTTPReq
  [[line [meth path ver]
    :sep b"\r\n"
    :from (.join " " it)
    :to (.split it)]
   [struct [headers]
    :struct HTTPHeaders]])

(defstruct HTTPResp
  [[line [ver status reason]
    :sep b"\r\n"
    :from (.join " " it)
    :to (.split it)]
   [struct [headers]
    :struct HTTPHeaders]])

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
   [struct [src] :struct (async-name IPv4Addr)]
   [struct [dst] :struct (async-name IPv4Addr)]])

(defstruct IPv6
  [[bits [ver tc fl] :lens [4 8 20]]
   [int plen :len 2]
   [int nh :len 1]
   [int hlim :len 1]
   [struct [src] :struct (async-name IPv6Addr)]
   [struct [dst] :struct (async-name IPv6Addr)]])

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
