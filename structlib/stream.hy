(require
  hyrule :readers * *
  asyncrule *)

(import io [BytesIO])

(defclass StreamError [Exception])

(defclass BufferOverflowError [StreamError]
  (defn __init__ [self exactly atmost]
    (.__init__ (super) (.format "buffer overflow error: {}/{}" exactly atmost))
    (setv self.exactly exactly
          self.atmost  atmost)))

(defclass IncompleteReadError [StreamError]
  (defn __init__ [self exactly atleast]
    (.__init__ (super) (.format "incomplete read error: {}/{}" exactly atleast))
    (setv self.exactly exactly
          self.atleast atleast)))

(async-defclass StreamReader []
  (setv read-buf-size (do-mac (<< 1 16)))

  (defn __init__ [self [buf b""]]
    (setv self.read-buf buf
          self.read-eof False))

  (defn read-buf-pop [self n]
    (let [buf self.read-buf]
      (cond (= n 0)
            b""
            (< 0 n (len self.read-buf))
            (do
              (setv self.read-buf (cut buf n None))
              (cut buf n))
            True
            (do
              (setv self.read-buf b"")
              buf))))

  (defn read-buf-unpop [self buf]
    (setv self.read-buf (+ buf self.read-buf)))

  (async-defn read1 [n]
    (raise NotImplementedError))

  (async-defn read [self [n 4096]]
    (when (> n self.read-buf-size)
      (raise (BufferOverflowError n self.read-buf-size)))
    (cond self.read-buf
          (.read-buf-pop self n)
          self.read-eof
          b""
          True
          (let [buf (async-wait (.read1 self n))]
            (unless buf
              (setv self.read-eof True))
            buf)))

  (async-defn read-all [self]
    (let [bufs (list)
          buf (async-wait (.read self))]
      (while buf
        (.append bufs buf)
        (setv buf (async-wait (.read self))))
      (.join b"" bufs)))

  (async-defn read-atleast [self n]
    (when (> n self.read-buf-size)
      (raise (BufferOverflowError n self.read-buf-size)))
    (let [bufs (list)]
      (while (> n 0)
        (let [buf (async-wait (.read self))]
          (unless buf
            (let [nread (sum (map len bufs))]
              (raise (IncompleteReadError nread (+ nread n)))))
          (.append bufs buf)
          (-= n (len buf))))
      (.join b"" bufs)))

  (async-defn read-exactly [self n]
    (let [buf (async-wait (.read-atleast self n))]
      (if (> (len buf) n)
          (do
            (.read-buf-unpop self (cut buf n None))
            (cut buf n))
          buf)))

  (async-defn read-until [self [sep b"\r\n"]]
    (let [buf (async-wait (.read-atleast self 1))
          sp (.split buf sep 1)]
      (while (= (len sp) 1)
        (+= buf (async-wait (.read-atleast self 1)))
        (when (> (len buf) self.read-buf-size)
          (raise (BufferOverflowError (len buf) self.read-buf-size)))
        (setv sp (.split buf sep 1)))
      (.read-buf-unpop self (get sp 1))
      (get sp 0)))

  (async-defn peek [self [n 4096]]
    (let [buf (async-wait (.read self n))]
      (.read-buf-unpop self buf)
      buf))

  (async-defn peek-atleast [self n]
    (let [buf (async-wait (.read-atleast self n))]
      (.read-buf-unpop self buf)
      buf)))

(async-defclass BIOStreamReader [(async-name StreamReader)]
  (defn __init__ [self [bio b""] #** kwargs]
    (.__init__ (super) #** kwargs)
    (setv self.bio (BytesIO bio)))

  (async-defn read1 [self n]
    (.read self.bio n)))

(export
  :objects [StreamError BufferOverflowError IncompleteReadError
            StreamReader AsyncStreamReader BIOStreamReader AsyncBIOStreamReader])
