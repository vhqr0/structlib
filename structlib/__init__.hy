(require
  hyrule :readers * *
  asyncrule :readers * *)

(import
  structlib.stream *)

(defn int-pack [i ilen]
  (.to-bytes i ilen "big"))

(defn int-unpack [b]
  (int.from-bytes b "big"))

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
    `(setv ~self.name-raw (let [data ~self.pack-form]
                            (+ (int-pack (len data) ~self.len) data))))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (let [dlen (int-unpack (async-wait (.read-exactly reader ~self.len)))]
                            (async-wait (.read-exactly reader dlen)))
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
    `(setv ~self.name-raw (async-wait (.unpack ~self.struct reader))
           ~self.name ~self.unpack-form)))

(defclass StructInField [_StructNamesMixin _StructMixin Field]
  (setv field-type 'structin)

  (defn pack-setv-form [self]
    `(setv ~self.name #(~@self.struct-names)
           ~self.name-raw (.pack ~self.struct #* ~self.pack-form)))

  (defn unpack-setv-form [self]
    `(setv ~self.name-raw (async-wait (.unpack ~self.struct reader))
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

(async-defclass Struct []
  (async-defn [classmethod] unpack [cls buf]
    (async-wait (cls.unpack-from-stream ((async-name BIOStreamReader) buf)))))

(defmacro defstruct [name fields]
  (let [fields (lfor field fields (#/ structlib.Field.from-model field))
        struct-names (let [names (list)] (for [field fields] (.struct-append-names field names)) names)
        pack-names (let [names (list)] (for [field fields] (.pack-append-names field names)) names)]
    `(async-defclass ~name [(async-name Struct)]
       (defn [staticmethod] pack [~@struct-names]
         ~@(gfor field fields (.pack-setv-form field))
         (.join b"" #(~@pack-names)))
       (async-defn [staticmethod] unpack-from-stream [reader]
         ~@(gfor field fields (.unpack-setv-form field))
         #(~@struct-names)))))

(export
  :objects [int-pack int-unpack bits-pack bits-unpack Struct AsyncStruct]
  :macros [defstruct])
