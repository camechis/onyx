(ns onyx.messaging.serializers.heartbeat-encoder
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])          
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.messaging.serializers.heartbeat-decoder]
           
           ))

(defprotocol PEncoder
  (set-epoch [this epoch])
  (set-dst-peer-id [this peer-id])
  (set-src-peer-id [this peer-id])
  (set-session-id [this session-id])
  (set-opts-map-bytes [this bs])
  (offset [this])
  (length [this])
  (wrap [this offset]))

(def coordinator (byte 0))
(def peer (byte 1))

(defn type->byte [v] 
  (get {:coordinator coordinator
        :task peer}
       v))

(deftype Encoder [^UnsafeBuffer buffer ^:unsynchronized-mutable offset]
  PEncoder
  (set-epoch [this epoch]
    (.putLong buffer offset (long epoch))
    this)
  (set-src-peer-id [this [peer-type peer-id]]
    (assert offset)
    (assert peer-type)
    (assert peer-id)
    (.putByte buffer (unchecked-add-int offset 8) (type->byte peer-type))
    (.putLong buffer (unchecked-add-int offset 9) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 17) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-dst-peer-id [this [peer-type peer-id]]
    (.putByte buffer (unchecked-add-int offset 25) (type->byte peer-type))
    (.putLong buffer (unchecked-add-int offset 26) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 34) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-session-id [this session-id]
    (.putLong buffer (unchecked-add-int offset 42) (long session-id))
    this)
  (set-opts-map-bytes [this bs]
    (.putShort buffer (unchecked-add-int offset 50) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 52) ^bytes bs))
  (offset [this] offset)
  (length [this] 
    (unchecked-add-int 52 (.getShort buffer (unchecked-add-int offset 50))))
  (wrap [this new-offset] 
    (set! offset new-offset)
    (.putShort buffer (unchecked-add-int new-offset 50) (short 0))
    this))
