(ns onyx.messaging.aeron.endpoint-status
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.serialize :as sz]
            [onyx.messaging.serializers.segment-decoder :as segment-decoder]
            [onyx.messaging.serializers.base-decoder :as base-decoder]
            [onyx.messaging.serializers.heartbeat-decoder :as heartbeat-decoder]
            [onyx.peer.constants :refer [initialize-epoch]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns]]
            [taoensso.timbre :refer [debug info warn trace] :as timbre])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent UnsafeBuffer]))

;; FIXME to be tuned
(def fragment-limit-receiver 1000)

(defn initial-statuses [peer-ids]
  (->> peer-ids
       (map (fn [p] 
              [p {:ready? false
                  :checkpointing? false
                  :heartbeat (System/nanoTime)
                  :epoch initialize-epoch
                  :min-epoch initialize-epoch}]))
       (into {})))

(defn statuses->ready? [statuses]
  (not (some false? (map (comp :ready? val) statuses))))

(defn statuses->min-epoch [statuses]
  (reduce min (map :epoch (vals statuses))))

(deftype EndpointStatus 
  [peer-config peer-id session-id ^Aeron conn ^Subscription subscription error
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch 
   ^:unsynchronized-mutable statuses        ^:unsynchronized-mutable ready
   ^:unsynchronized-mutable min-epoch]
  onyx.messaging.protocols.endpoint-status/EndpointStatus
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (reset! error x)))
          media-driver-dir ^String (:onyx.messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName media-driver-dir))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          sub (.addSubscription conn channel heartbeat-stream-id)]
      (info "Started endpoint status on peer:" peer-id)
      (EndpointStatus. peer-config peer-id session-id conn sub error 
                       replica-version epoch statuses min-epoch ready)))
  (stop [this]
    (info "Stopping endpoint status" [peer-id])
    (try
     (.close subscription)
     (catch Throwable t
       (info "Error closing endpoint subscription:" t)))
     (.close conn)
    (EndpointStatus. peer-config peer-id session-id nil nil error nil nil nil nil false))
  (info [this]
    [:rv replica-version
     :e epoch
     :channel-id (.channel subscription)
     :registration-id (.registrationId subscription)
     :stream-id (.streamId subscription)
     :closed? (.isClosed subscription)
     :images (mapv autil/image->map (.images subscription))
     :statuses statuses
     :ready? ready])
  (poll! [this]
    (when @error (throw @error))
    (.poll ^Subscription subscription ^FragmentHandler this fragment-limit-receiver))
  (set-endpoint-peers! [this expected-peers]
    (->> expected-peers
         (initial-statuses)
         (set! statuses))
    (set! min-epoch (statuses->min-epoch statuses))
    (set! ready (statuses->ready? statuses))
    this)
  (ready? [this]
    ready)
  (statuses [this]
    statuses)
  (min-endpoint-epoch [this]
    min-epoch)
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (set! replica-version new-replica-version)
    (->> (keys statuses)
         (initial-statuses)
         (set! statuses))
    (set! min-epoch (statuses->min-epoch statuses))
    (set! ready (statuses->ready? statuses))
    this)
  FragmentHandler
  (onFragment [this buffer offset length header]
    (let [base-dec (base-decoder/->Decoder buffer offset)
          ;;; EWWW
          hb-dec (sz/wrap-other buffer offset)]
      (when (and (= session-id (heartbeat-decoder/get-session-id hb-dec)) 
                 (= replica-version (base-decoder/get-replica-version base-dec)))
        (let [dst-peer-id (sz/decoder->dst-peer-id hb-dec)] 
          (case (int (base-decoder/get-type base-dec))
            2 (when (= peer-id dst-peer-id)
                (let [message (sz/deserialize buffer offset)
                      src-peer-id (:src-peer-id message)
                      epoch (:epoch message)
                      peer-status (or (get statuses src-peer-id) 
                                      (throw (Exception. "Heartbeating peer does not exist for this replica-version.")))
                      prev-epoch (:epoch peer-status)]
                  (when-not (or (= epoch (inc prev-epoch))
                                (= epoch prev-epoch))
                    (throw (ex-info "Received epoch is not in sync with expected epoch." 
                                    {:our-replica-version replica-version
                                     :prev-epoch prev-epoch
                                     :epoch epoch
                                     :message message})))

                  (->> (update statuses src-peer-id merge {:checkpointing? (:checkpointing? message)
                                                           :replica-version (:replica-version message)
                                                           :epoch (:epoch message)
                                                           :drained? (if (contains? message :drained?)
                                                                       (:drained? message)
                                                                       false)
                                                           :min-epoch (:min-epoch message)
                                                           :heartbeat (System/nanoTime)}) 
                       (set! statuses))
                  (set! min-epoch (statuses->min-epoch statuses))))

            4 (when (= peer-id dst-peer-id)
                (let [message (sz/deserialize buffer offset)
                      src-peer-id (:src-peer-id message)] 
                  (->> (update statuses src-peer-id merge {:ready? true 
                                                           :heartbeat (System/nanoTime)}) 
                       (set! statuses))
                  (set! ready (statuses->ready? statuses))))

            (throw (ex-info "Invalid message type" {:message (sz/deserialize buffer offset)}))))))))

(defn new-endpoint-status [peer-config peer-id session-id]
  (->EndpointStatus peer-config peer-id session-id nil nil (atom nil) nil nil nil nil false)) 
