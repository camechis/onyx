(ns onyx.messaging.serializers.heartbeat-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-epoch [this])
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
  (get-dst-peer-type [this]
    (.getByte buffer (unchecked-add-int offset 8)))
  (get-dst-peer-id [this]
    (java.util.UUID. (.getLong buffer (unchecked-add-int offset 9))
                     (.getLong buffer (unchecked-add-int offset 17))))
  (get-session-id [this]
    (.getLong buffer (unchecked-add-int offset 25)))
  (get-opts-map-bytes [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 33)))] 
      (.getBytes buffer (unchecked-add-int offset 35) bs)
      bs))
  (wrap [this new-buffer new-offset]
    (set! buffer new-buffer)
    (set! offset new-offset)
    this))
