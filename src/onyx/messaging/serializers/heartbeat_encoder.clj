(ns onyx.messaging.serializers.heartbeat-encoder
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])          
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.messaging.serializers.heartbeat-decoder]
           
           ))

(defprotocol PEncoder
  (set-epoch [this epoch])
  (set-dst-peer-type [this peer-id])
  (set-dst-peer-id [this peer-id])
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
  (set-dst-peer-type [this type]
    (.putByte buffer (unchecked-add-int offset 8) (byte type))
    this)
  (set-dst-peer-id [this peer-id]
    (.putLong buffer (unchecked-add-int offset 9) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 17) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-session-id [this session-id]
    (.putLong buffer (unchecked-add-int offset 25) (long session-id))
    this)
  (set-opts-map-bytes [this bs]
    (.putShort buffer (unchecked-add-int offset 33) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 35) ^bytes bs))
  (offset [this] offset)
  (length [this] 
    (unchecked-add-int 35 (.getShort buffer (unchecked-add-int offset 33))))
  (wrap [this new-offset] 
    (set! offset new-offset)
    (.putShort buffer (unchecked-add-int new-offset 33) (short 0))
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
