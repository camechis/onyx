(ns ^:no-doc onyx.peer.replica
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.extensions :as extensions]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.log.entry :refer [create-log-entry]]))

(defn send-to-outbox [outbox-ch reactions]
  (doseq [reaction reactions]
    (>!! outbox-ch reaction)))

(defn annotate-reaction [{:keys [message-id]} id entry]
  (let [peer-annotated (assoc entry :peer-parent id)]
    ;; Not all messages are derived from other messages.
    ;; For instance, :prepare-join-cluster is a "root"
    ;; message.
    (if message-id
      (assoc peer-annotated :entry-parent message-id)
      peer-annotated)))

(defn transition-peers [log entry old-replica new-replica diff peer-states peer-config]
  (reduce
   (fn [result [id {:keys [peer-replica-view] :as peer-state}]]
     (let [rs (extensions/reactions entry old-replica new-replica diff peer-state)
           annotated-rs (map (partial annotate-reaction entry (:id peer-state)) rs)
           new-peer-view (extensions/peer-replica-view log entry old-replica new-replica peer-replica-view diff peer-state peer-config)
           new-state (extensions/fire-side-effects! entry old-replica new-replica diff peer-state)
           new-state (assoc new-state :peer-replica-view new-peer-view)]
       (-> result
           (update-in [:reactions] into annotated-rs)
           (assoc-in [:states id] new-state))))
   {:reactions []
    :states {}}
   peer-states))

(defn transition-group [log entry old-replica new-replica diff group-state]
  (let [rs (extensions/reactions entry old-replica new-replica diff group-state)
        annotated-rs (map (partial annotate-reaction entry (:id group-state)) rs)
        new-state (extensions/fire-side-effects! entry old-replica new-replica diff group-state)]
    {:reactions annotated-rs
     :updated-group-state new-state}))

(defn processing-loop
  [group-id log monitoring replica-atom inbox-ch outbox-ch
   component-kill-ch restart-ch peer-states peer-config]
  (try
    (loop [group-state {:id group-id
                        :opts peer-config
                        :log log
                        :monitoring monitoring}]
      (let [replica @replica-atom
            [entry ch] (alts!! [component-kill-ch inbox-ch] :priority true)]
        (when (and (= ch inbox-ch) entry)
          (let [new-replica (extensions/apply-log-entry entry replica)
                diff (extensions/replica-diff entry replica new-replica)]
            (if (extensions/multiplexed-entry? entry)
              (let [{:keys [reactions updated-group-state]}
                    (transition-group log entry replica new-replica diff group-state)]
                (doseq [[peer-id state] @peer-states]
                  (let [new-peer-view (extensions/peer-replica-view
                                       log entry replica new-replica
                                       (:peer-replica-view state) diff
                                       state peer-config)
                        new-state (assoc state :peer-replica-view new-peer-view)]
                    (swap! peer-states assoc peer-id new-state)))
                (reset! replica-atom new-replica)
                (send-to-outbox outbox-ch reactions)
                (recur updated-group-state))
              (let [{:keys [reactions states]} (transition-peers log entry replica new-replica diff @peer-states peer-config)]
                (doseq [[peer-id new-state] states]
                  (swap! peer-states assoc peer-id new-state))
                (reset! replica-atom new-replica)
                (send-to-outbox outbox-ch reactions)
                (recur group-state)))))))
    (catch Throwable e
      (error e "Error in Replica Controller processing loop.")
      (close! restart-ch))
    (finally
      (info "Replica Controller finished processing loop."))))

(defn outbox-loop [log outbox-ch restart-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
        (extensions/write-log-entry log entry)
        (catch Throwable e
          (warn e "Replica services couldn't write to ZooKeeper.")
          (close! restart-ch)))
      (recur))))

(defrecord ReplicaSubscription [peer-config]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Replica Subscription")
    ;; Race to write the job scheduler and messaging to durable storage so that
    ;; non-peers subscribers can discover which properties to use.
    ;; Only one writer will succeed, and only one needs to.
    (extensions/write-chunk log :job-scheduler {:job-scheduler (:onyx.peer/job-scheduler peer-config)} nil)
    (extensions/write-chunk log :messaging {:messaging (select-keys peer-config [:onyx.messaging/impl])} nil)

    (let [group-id (java.util.UUID/randomUUID)
          inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity peer-config))
          origin (extensions/subscribe-to-log log inbox-ch)]
      (assoc component
             :group-id group-id
             :inbox-ch inbox-ch
             :replica (atom origin))))

  (stop [component]
    (taoensso.timbre/info "Stopping Replica Subscription")
    (close! (:inbox-ch component))
    component))

(defn replica-subscription [peer-config]
  (->ReplicaSubscription peer-config))

(defrecord ReplicaController [peer-config restart-ch]
  component/Lifecycle

  (start [{:keys [log monitoring replica-subscription] :as component}]
    (taoensso.timbre/info "Starting Replica Controller")
    (let [group-id (:group-id replica-subscription)
          outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity peer-config))
          component-kill-ch (promise-chan)
          peer-states (atom {})
          entry (create-log-entry :prepare-join-cluster
                                  {:joiner group-id})
          outbox-loop-ch (thread (outbox-loop log outbox-ch restart-ch))
          processing-loop-ch
          (thread
            (processing-loop group-id log monitoring
                             (:replica replica-subscription)
                             (:inbox-ch replica-subscription)
                             outbox-ch component-kill-ch restart-ch
                             peer-states peer-config))]
      (extensions/register-pulse log group-id)
      (>!! outbox-ch entry)
      (assoc component
             :log log
             :outbox-ch outbox-ch
             :outbox-loop-ch outbox-loop-ch
             :processing-loop-ch processing-loop-ch
             :component-kill-ch component-kill-ch
             :group-id group-id
             :peer-states peer-states)))

  (stop [component]
    (taoensso.timbre/info "Stopping Replica Controller")

    (close! (:outbox-ch component))
    (close! (:component-kill-ch component))
    (<!! (:outbox-loop-ch component))
    (<!! (:processing-loop-ch component))

    component))

(defn replica-controller [peer-config restart-ch]
  (->ReplicaController peer-config restart-ch))