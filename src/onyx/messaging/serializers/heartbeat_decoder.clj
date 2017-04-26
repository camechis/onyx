(ns onyx.messaging.serializers.heartbeat-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-epoch [this])
  (get-src-peer-id [this])
  (get-dst-peer-type [this])
  (get-dst-peer-id [this])
  (get-session-id [this])
  (get-opts-map-bytes [this])
  (wrap [this ^UnsafeBuffer buffer offset]))

(deftype Decoder [^:unsynchronized-mutable ^UnsafeBuffer buffer
                  ^:unsynchronized-mutable offset]
  PDecoder
  (get-epoch [this]
    (.getLong buffer offset))
  (get-src-peer-id [this]
    (java.util.UUID. (.getLong buffer (unchecked-add-int offset 8))
                     (.getLong buffer (unchecked-add-int offset 16))))
  (get-dst-peer-type [this]
    (.getByte buffer (unchecked-add-int offset 24)))
  (get-dst-peer-id [this]
    (java.util.UUID. (.getLong buffer (unchecked-add-int offset 25))
                     (.getLong buffer (unchecked-add-int offset 33))))
  (get-session-id [this]
    (.getLong buffer (unchecked-add-int offset 41)))
  (get-opts-map-bytes [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 49)))] 
      (.getBytes buffer (unchecked-add-int offset 51) bs)
      bs))
  (wrap [this new-buffer new-offset]
    (set! buffer new-buffer)
    (set! offset new-offset)
    this))
