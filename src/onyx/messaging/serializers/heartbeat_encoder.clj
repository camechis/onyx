(ns onyx.messaging.serializers.heartbeat-encoder
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])          
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.messaging.serializers.heartbeat-decoder]
           
           ))

(defprotocol PEncoder
  (set-epoch [this epoch])
  (set-dst-peer-type [this peer-id])
  (set-dst-peer-id [this peer-id])
  (set-src-peer-id [this peer-id])
  (set-session-id [this session-id])
  (set-opts-map-bytes [this bs])
  (offset [this])
  (length [this])
  (wrap [this offset]))

(deftype Encoder [^UnsafeBuffer buffer ^:unsynchronized-mutable offset]
  PEncoder
  (set-epoch [this epoch]
    (.putLong buffer offset (long epoch))
    this)
  (set-src-peer-id [this src-peer-id]
    (.putLong buffer (unchecked-add-int offset 8) (.getMostSignificantBits ^java.util.UUID src-peer-id))
    (.putLong buffer (unchecked-add-int offset 16) (.getLeastSignificantBits ^java.util.UUID src-peer-id))
    this)
  (set-dst-peer-type [this type]
    (.putByte buffer (unchecked-add-int offset 24) (byte type))
    this)
  (set-dst-peer-id [this peer-id]
    (.putLong buffer (unchecked-add-int offset 25) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 33) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-session-id [this session-id]
    (.putLong buffer (unchecked-add-int offset 41) (long session-id))
    this)
  (set-opts-map-bytes [this bs]
    (.putShort buffer (unchecked-add-int offset 49) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 51) ^bytes bs))
  (offset [this] offset)
  (length [this] 
    (unchecked-add-int 51 (.getShort buffer (unchecked-add-int offset 49))))
  (wrap [this new-offset] 
    (set! offset new-offset)
    (.putShort buffer (unchecked-add-int new-offset 49) (short 0))
    this))

(comment
 (def buf (UnsafeBuffer. (byte-array 2000)))

 (def uuid (java.util.UUID/randomUUID))
 (def enced (-> (->Encoder buf 0)
                (wrap 0)
                (set-epoch 33)
                (set-dst-peer-id uuid)
                (set-session-id 88888)
                (set-opts-map-bytes (messaging-compress {:a 3}))))

 ;#uuid "5f857339-c5ca-40d6-bc6a-175351121cc2"

 (-> (onyx.messaging.serializers.heartbeat-decoder/->Decoder buf 0)
     (onyx.messaging.serializers.heartbeat-decoder/wrap buf 0)
     ;(onyx.messaging.serializers.heartbeat-decoder/get-dst-peer-id)
     ;(onyx.messaging.serializers.heartbeat-decoder/get-epoch)
     (onyx.messaging.serializers.heartbeat-decoder/get-opts-map-bytes)
     (messaging-decompress)

     ))
