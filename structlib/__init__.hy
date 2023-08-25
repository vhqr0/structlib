(require
  hyrule :readers * *
  asyncrule :readers * *)

(import
  structlib.stream *)

(defn bytes-concat [g]
  (.join b"" g))

(defn int-pack [i ilen]
  (.to-bytes i ilen "big"))

(defn int-unpack [b]
  (int.from-bytes b "big"))

(defn varlen-pack [data ilen]
  (+ (int-pack (len data) ilen) data))

(async-defclass VarLenUnpacker []
  (async-defn [staticmethod] unpack-from-stream [reader ilen]
    (let [dlen (int-unpack (async-wait (.read-exactly reader ilen)))]
      (async-wait (.read-exactly reader dlen)))))

(defn bits-pack [offsets bits ilen]
  (-> (cfor sum
            #(bit offset) (zip bits offsets)
            (<< bit offset))
      (int-pack ilen)))

(defn bits-unpack [offsets masks b]
  (let [i (int-unpack b)]
    (gfor #(offset mask) (zip offsets masks)
          (& (>> i offset) mask))))

(defclass Field []
  (setv field-type-dict (dict)
        field-type None)

  (defn __init-subclass__ [cls]
    (when cls.field-type
      (setv (get cls.field-type-dict cls.field-type) cls)))

  (defn [classmethod] from-model [cls model]
    (let [model (#/ collections.deque model)
          type (.popleft model)
          name (.popleft model)
          meta (let [meta (dict)]
                 (while model
                   (let [k (.popleft model)
                         v (.popleft model)]
                     (setv (get meta k.name) v)))
                 meta)]
      ((get cls.field-type-dict type) :name name :meta meta)))

  (defn __init__ [self name meta]
    (setv self.name name
          self.name-raw (hy.models.Symbol (+ (str name) "-raw"))
          self.meta meta
          self.pack-form (.get self.meta "pack-form" self.name)
          self.unpack-form (.get self.meta "unpack-form" self.name-raw)))

  (defn struct-append-names [self names]
    (.append names self.name))

  (defn pack-append-names [self names]
    (.append names self.name-raw))

  (defn pack-setv-form [self]
    (raise NotImplementedError))

  (defn unpack-setv-form [self]
    (raise NotImplementedError)))

(defclass _LenMixin [Field]
  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.len (get self.meta "len"))))

(defclass _SepMixin [Field]
  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.sep (get self.meta "sep"))))

(defclass _StructMixin [Field]
  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.struct (get self.meta "struct"))))

(defclass _StructNamesMixin [Field]
  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.struct-names (get self.meta "struct-names")))

  (defn struct-append-names [self names]
    (for [name self.struct-names]
      (.append names name))))

(defclass _RepeatMixin [Field]
  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.repeat (get self.meta "repeat"))))

(defclass _StopFormMixin [Field]
  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.stop-form (.get self.meta "stop-form" '(not (async-wait (.peek reader)))))))

(defclass BytesField [_LenMixin Field]
  (setv field-type 'bytes)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw ~self.pack-form))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (async-wait (.read-exactly reader ~self.len))
           ~self.name ~self.unpack-form)))

(defclass IntField [_LenMixin Field]
  (setv field-type 'int)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (int-pack ~self.pack-form ~self.len)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (int-unpack (async-wait (.read-exactly reader ~self.len)))
           ~self.name ~self.unpack-form)))

(defclass VarLenField [_LenMixin Field]
  (setv field-type 'varlen)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (varlen-pack ~self.pack-form ~self.len)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (.unpack-from-stream (async-name VarLenUnpacker) reader ~self.len)
           ~self.name ~self.unpack-form)))

(defclass LineField [_SepMixin Field]
  (setv field-type 'line)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (+ (.encode ~self.pack-form) ~self.sep)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (.decode (async-wait (.read-until reader :sep ~self.sep)))
           ~self.name ~self.unpack-form)))

(defclass StructField [_StructMixin Field]
  (setv field-type 'struct)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (.pack ~self.struct #* ~self.pack-form)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (async-wait (.unpack-from-stream ~self.struct reader))
           ~self.name ~self.unpack-form)))

(defclass StructSingleField [_StructMixin Field]
  (setv field-type 'struct-single)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (.pack ~self.struct ~self.pack-form)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (get (async-wait (.unpack-from-stream ~self.struct reader)) 0)
           ~self.name ~self.unpack-form)))

(defclass StructInlineField [_StructNamesMixin _StructMixin Field]
  (setv field-type 'struct-inline)

  (defn pack-setv-form [self]
    `(setv ~self.name #(~@self.struct-names)
           ~self.name-raw (.pack ~self.struct #* ~self.pack-form)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (async-wait (.unpack-from-stream ~self.struct reader))
           ~self.name ~self.unpack-form
           #(~@self.struct-names) ~self.name)))

(defclass BitsField [_StructNamesMixin Field]
  (setv field-type 'bits)

  (defn __init__ [self #* args #** kwargs]
    (.__init__ (super) #* args #** kwargs)
    (setv self.lens (list (map int (get self.meta "lens"))))
    (setv s (sum self.lens)
          #(d m) (divmod s 8))
    (unless (= m 0)
      (raise ValueError))
    (setv self.len d)
    (setv self.masks (lfor len self.lens (- (<< 1 len) 1)))
    (setv self.offsets (list))
    (for [len self.lens]
      (-= s len)
      (.append self.offsets s)))

  (defn pack-setv-form [self]
    `(setv ~self.name #(~@self.struct-names)
           ~self.name-raw (bits-pack #(~@self.offsets) ~self.pack-form ~self.len)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (bits-unpack #(~@self.offsets) #(~@self.masks) (async-wait (.read-exactly reader ~self.len)))
           ~self.name ~self.unpack-form
           #(~@self.struct-names) ~self.name)))

(defclass RepeatBytesField [_RepeatMixin _LenMixin Field]
  (setv field-type 'repeat-bytes)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (bytes-concat ~self.pack-form)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (lfor _ (range ~self.repeat)
                                (async-wait (.read-exactly reader ~self.len)))
           ~self.name ~self.unpack-form)))

(defclass RepeatIntField [_RepeatMixin _LenMixin Field]
  (setv field-type 'repeat-int)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (bytes-concat (gfor data ~self.pack-form (int-pack data ~self.len)))))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (lfor _ (range ~self.repeat)
                                (int-unpack (async-wait (.read-exactly reader ~self.len))))
           ~self.name ~self.unpack-form)))

(defclass RepeatVarlenField [_RepeatMixin _LenMixin Field]
  (setv field-type 'repeat-varlen)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (bytes-concat (gfor data ~self.pack-form (varlen-pack data ~self.len)))))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (lfor _ (range ~self.repeat)
                                (async-wait (.unpack-from-stream (async-name VarLenUnpacker) reader ~self.len)))
           ~self.name ~self.unpack-form)))

(defclass RepeatLineField [_RepeatMixin _SepMixin Field]
  (setv field-type 'repeat-line)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (bytes-concat (gfor data ~self.pack-form (+ (.encode data) ~self.sep)))))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (lfor _ (range ~self.repeat)
                                (.decode (async-wait (.read-until reader :sep ~self.sep))))
           ~self.name ~self.unpack-form)))

(defclass RepeatStructField [_RepeatMixin _StructMixin Field]
  (setv field-type 'repeat-struct)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (bytes-concat (gfor data ~self.pack-form (.pack ~self.struct #* data)))))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (lfor _ (range ~self.repeat)
                                (async-wait (.unpack-from-stream ~self.struct reader)))
           ~self.name ~self.unpack-form)))

(defclass DryBytesField [_StopFormMixin _LenMixin Field]
  (setv field-type 'dry-bytes)

  (defn pack-setv-form [self]
    `(setv ~self.name-raw (bytes-concat ~self.pack-form)))

  (defn unpack-setv-form [self]
    `(do
       (setv ~self.name-raw [])
       (while True
         (let [it (async-wait (.read-exactly reader ~self.len))]
           (.append ~self.name-raw it)
           (when ~self.stop-form
             (break))))
       (setv ~self.name ~self.unpack-form))))

(async-defclass Struct []
  (setv struct-names None)

  (defn [staticmethod] pack [#* args]
    (raise NotImplementedError))

  (async-defn [staticmethod] unpack-from-stream [reader]
    (raise NotImplementedError))

  (async-defn [classmethod] pack-dict [cls d]
    (.pack cls #* (gfor name cls.struct-names (get d name))))

  (async-defn [classmethod] unpack [cls buf]
    (let [reader ((async-name BIOStreamReader) buf)
          st (async-wait (cls.unpack-from-stream reader))]
      (let [buf (async-wait (.peek reader))]
        (when (async-wait (.peek reader))
          (raise (IncompleteReadError 0 (len buf)))))
      st))

  (async-defn [classmethod] unpack-dict-from-stream [cls reader]
    (dict (zip cls.struct-names (async-wait (.unpack-from-stream cls reader)))))

  (async-defn [classmethod] unpack-dict [cls buf]
    (dict (zip cls.struct-names (async-wait (.unpack cls buf))))))

(defmacro defstruct [name fields]
  (let [fields (lfor field fields (#/ structlib.Field.from-model field))
        struct-names (let [names (list)] (for [field fields] (.struct-append-names field names)) names)
        pack-names (let [names (list)] (for [field fields] (.pack-append-names field names)) names)]
    `(async-defclass ~name [(async-name Struct)]
       (setv struct-names #(~@(gfor name struct-names (hy.mangle (str name)))))
       (defn [staticmethod] pack [~@struct-names]
         ~@(gfor field fields (.pack-setv-form field))
         (bytes-concat #(~@pack-names)))
       (async-defn [staticmethod] unpack-from-stream [reader]
         ~@(gfor field fields (.unpack-setv-form field))
         #(~@struct-names)))))

(export
  :objects [bytes-concat int-pack int-unpack varlen-pack VarLenUnpacker bits-pack bits-unpack Struct AsyncStruct]
  :macros [defstruct])
