(ns onyx.messaging.serialize
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.types :refer [barrier ready ready-reply heartbeat]]
            [onyx.messaging.serializers.heartbeat-encoder :as heartbeat-encoder]
            [onyx.messaging.serializers.heartbeat-decoder :as heartbeat-decoder]
            [onyx.messaging.serializers.base-decoder :as base-decoder]
            [onyx.messaging.serializers.base-encoder :as base-encoder])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.messaging.serializers.base_decoder.Decoder]))


(def message-id ^:const (byte 0))
(def barrier-id ^:const (byte 1))
(def heartbeat-id ^:const (byte 2))
(def ready-id ^:const (byte 3))
(def ready-reply-id ^:const (byte 4))
(def coordinator (byte 0))
(def peer (byte 1))

(defn decoder->dst-peer-id [decoder]
  (if (= coordinator (heartbeat-decoder/get-dst-peer-type decoder))
    [:coordinator (heartbeat-decoder/get-dst-peer-id decoder)]
    (heartbeat-decoder/get-dst-peer-id decoder)))

(defn wrap-other [^UnsafeBuffer buf offset]
  (let [decoder (base-decoder/wrap (base-decoder/->Decoder nil offset) buf offset)]
    (-> (heartbeat-decoder/->Decoder nil nil)
        (heartbeat-decoder/wrap buf (+ offset (base-decoder/length decoder))))))

(defn deserialize [^UnsafeBuffer buf offset]
  (let [decoder (base-decoder/wrap (base-decoder/->Decoder nil offset) buf offset)
        hb-dec (-> (heartbeat-decoder/->Decoder nil nil)
                   (heartbeat-decoder/wrap buf (+ offset (base-decoder/length decoder))))
        dst-peer-id (decoder->dst-peer-id hb-dec)]
    (merge {:type (base-decoder/get-type decoder)
            :replica-version (base-decoder/get-replica-version decoder)
            :short-id (base-decoder/get-dest-id decoder)
            :epoch (heartbeat-decoder/get-epoch hb-dec)
            :session-id (heartbeat-decoder/get-session-id hb-dec)
            :src-peer-id (heartbeat-decoder/get-src-peer-id hb-dec)
            :dst-peer-id dst-peer-id}
           (messaging-decompress (heartbeat-decoder/get-opts-map-bytes hb-dec)))))

(defn serialize [^UnsafeBuffer buf offset msg]
  (let [enc (base-encoder/->Encoder nil offset)
        enc (-> enc
                (base-encoder/wrap buf offset)
                (base-encoder/set-type (:type msg))
                (base-encoder/set-replica-version (:replica-version msg))
                (base-encoder/set-dest-id (:short-id msg)))
        hb-enc (heartbeat-encoder/->Encoder buf nil)
        dst-peer-id (:dst-peer-id msg)
        dst-peer-type (if (vector? dst-peer-id)
                        coordinator
                        peer)
        dst-peer (if (vector? dst-peer-id)
                   (second dst-peer-id)
                   dst-peer-id)]
    (println "SRC_PEER_ID" (:src-peer-id msg))
    (-> hb-enc
        (heartbeat-encoder/wrap (base-encoder/length enc))
        (heartbeat-encoder/set-epoch (:epoch msg))
        (heartbeat-encoder/set-session-id (:session-id msg))
        (heartbeat-encoder/set-src-peer-id (:src-peer-id msg))
        (heartbeat-encoder/set-dst-peer-type dst-peer-type)
        (heartbeat-encoder/set-dst-peer-id dst-peer)
        (heartbeat-encoder/set-opts-map-bytes (-> msg
                                                  (dissoc :type)
                                                  (dissoc :replica-version)
                                                  (dissoc :short-id)
                                                  (dissoc :epoch)
                                                  (dissoc :session-id)
                                                  (dissoc :dst-peer-id)
                                                  (messaging-compress))))
    (base-encoder/set-payload-length enc (heartbeat-encoder/length hb-enc))
    (assert (= msg (deserialize buf offset))
            [msg (deserialize buf offset)])
    (+ (heartbeat-encoder/length hb-enc) (base-encoder/length enc))))

(def buf (UnsafeBuffer. (byte-array 500)))
(serialize buf 
           0
           (onyx.types/heartbeat 33 44 
                                     (java.util.UUID/randomUUID)
                                     (java.util.UUID/randomUUID)
                                     33
                                     4))

#_(deserialize buf 0)
#_(deserialize buf 0)


