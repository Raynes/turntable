(ns flatland.turntable.timer
  (:require [clj-time.local :as ltime]
            [flatland.chronicle :refer [times-for]])
  (:import (java.util Timer TimerTask)))

;; TODO: Use a ScheduledExecutorService?

(defn timertask
  "Takes a function and returns a TimerTask object for passing to .schedule."
  [f]
  (proxy [TimerTask] []
    (run [] (f))))

(defn sequential-scheduler
  "Takes a Timer object, a function, and times to run and returns a
   a function that will run the function and add another scheduled task
   that does the same thing. This has the effect of infinitely scheduling
   tasks."
  [timer f times]
  (timertask
    (fn []
      (.schedule timer (sequential-scheduler timer f (rest times)) (.toDate (first times)))
      (f))))

(defn schedule
  "Schedule a task to run at the times specified by the chronicle map."
  [f spec]
  (let [[start & rest] (rest (times-for spec (ltime/local-now)))
        timer (Timer.)]
    (doto timer
      (.schedule (sequential-scheduler timer f rest) (.toDate start)))))
