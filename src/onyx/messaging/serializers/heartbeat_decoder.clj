(ns onyx.messaging.serializers.heartbeat-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-epoch [this])
  (get-src-peer-id [this])
  (get-dst-peer-id [this])
  (get-session-id [this])
  (get-opts-map-bytes [this])
  (wrap [this ^UnsafeBuffer buffer offset]))

(def coordinator (byte 0))
(def peer (byte 1))

(defn byte->type [v] 
  (get {coordinator :coordinator
        peer :task}
       v))

(deftype Decoder [^:unsynchronized-mutable ^UnsafeBuffer buffer
                  ^:unsynchronized-mutable offset]
  PDecoder
  (get-epoch [this]
    (.getLong buffer offset))
  (get-src-peer-id [this]
    [(byte->type (.getByte buffer (unchecked-add-int offset 8)))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 9))
                      (.getLong buffer (unchecked-add-int offset 17)))])
  (get-dst-peer-id [this]
    [(byte->type (.getByte buffer (unchecked-add-int offset 25)))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 26))
                      (.getLong buffer (unchecked-add-int offset 34)))])
  (get-session-id [this]
    (.getLong buffer (unchecked-add-int offset 42)))
  (get-opts-map-bytes [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 50)))] 
      (.getBytes buffer (unchecked-add-int offset 52) bs)
      bs))
  (wrap [this new-buffer new-offset]
    (set! buffer new-buffer)
    (set! offset new-offset)
    this))
