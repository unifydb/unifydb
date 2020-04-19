(ns unifydb.stringprep
  (:import [java.text Normalizer Normalizer$Form]))

(def saslprep-mapping
  "Unicode mapping for the SASLprep profile."
  {\u00A0 \u0020
   \u1680 \u0020
   \u2000 \u0020
   \u2001 \u0020
   \u2002 \u0020
   \u2003 \u0020
   \u2004 \u0020
   \u2005 \u0020
   \u2006 \u0020
   \u2007 \u0020
   \u2008 \u0020
   \u2009 \u0020
   \u200A \u0020
   \u200B \u0020
   \u202F \u0020
   \u205F \u0020
   \u3000 \u0020
   \u00AD ::nothing
   \u034F ::nothing
   \u1806 ::nothing
   \u180B ::nothing
   \u180C ::nothing
   \u180D ::nothing
   \u200C ::nothing
   \u200D ::nothing
   \u2060 ::nothing
   \uFE00 ::nothing
   \uFE01 ::nothing
   \uFE02 ::nothing
   \uFE03 ::nothing
   \uFE04 ::nothing
   \uFE05 ::nothing
   \uFE06 ::nothing
   \uFE07 ::nothing
   \uFE08 ::nothing
   \uFE09 ::nothing
   \uFE0A ::nothing
   \uFE0B ::nothing
   \uFE0C ::nothing
   \uFE0D ::nothing
   \uFE0E ::nothing
   \uFE0F ::nothing
   \uFEFF ::nothing})

(defn nfkc
  [input]
  (Normalizer/normalize input Normalizer$Form/NFKC))

(defn in-range
  "True if the `codepoint` falls within the range from `lower` to `upper`, inclusive."
  [codepoint lower upper]
  (and (>= codepoint lower)
       (<= codepoint upper)))

(def non-ascii-space
  #{0x00A0 0x1680 0x2000 0x2001 0x2002 0x2003 0x2004 0x2005 0x2006
    0x2007 0x2008 0x2009 0x200A 0x200B 0x202F 0x205F 0x3000})

(defn ascii-control
  [codepoint]
  (or (in-range codepoint 0x0000 0x001F)
      (= codepoint 0x007F)))

(defn non-ascii-control
  [codepoint]
  (or (in-range codepoint 0x0080 0x009F)
      (in-range codepoint 0x206A 0x206F)
      (in-range codepoint 0xFFF9 0xFFFC)
      (in-range codepoint 0x1D173 0x1d17A)
      (#{0x06DD 0x070F 0x180E 0x200C 0x200D 0x2028 0x2029 0x2060
         0x2061 0x2062 0x2063 0xFEFF} codepoint)))

(defn private-use
  [codepoint]
  (or (in-range codepoint 0xE000 0xF8FF)
      (in-range codepoint 0xF0000 0xFFFFD)
      (in-range codepoint 0x100000 0x10FFFD)))

(defn non-character-code-point
  [codepoint]
  (or (in-range codepoint 0xFDD0 0xFDEF)
      (in-range codepoint 0xFFFE 0xFFFF)
      (in-range codepoint 0x1FFFE 0x1FFFF)
      (in-range codepoint 0x2FFFE 0x2FFFF)
      (in-range codepoint 0x3FFFE 0x3FFFF)
      (in-range codepoint 0x4FFFE 0x4FFFF)
      (in-range codepoint 0x5FFFE 0x5FFFF)
      (in-range codepoint 0x6FFFE 0x6FFFF)
      (in-range codepoint 0x7FFFE 0x7FFFF)
      (in-range codepoint 0x8FFFE 0x8FFFF)
      (in-range codepoint 0x9FFFE 0x9FFFF)
      (in-range codepoint 0xAFFFE 0xAFFFF)
      (in-range codepoint 0xBFFFE 0xBFFFF)
      (in-range codepoint 0xCFFFE 0xCFFFF)
      (in-range codepoint 0xDFFFE 0xDFFFF)
      (in-range codepoint 0xEFFFE 0xEFFFF)
      (in-range codepoint 0xFFFFE 0xFFFFF)
      (in-range codepoint 0x10FFFE 0x10FFFF)))

(defn surrogate-code
  [codepoint]
  (in-range codepoint 0xD800 0xDFFF))

(def inappropriate-for-plain-text
  #{0xFFF9 0xFFFA 0xFFFB 0xFFFC 0xFFFD})

(defn inappropriate-for-canonical-representation
  [codepoint]
  (in-range codepoint 0x2FF0 0x2FFB))

(def change-display-properties-or-deprecated
  #{0x0340 0x0341 0x200E 0x200F 0x202A 0x202B 0x202C 0x202D 0x202E
    0x206A 0x206B 0x206C 0x206D 0x206E 0x206F})

(defn tagging-character
  [codepoint]
  (or (in-range codepoint 0xE0020 0xE007F)
      (#{0xE0001} codepoint)))

(defn saslprep-prohibited
  "Prohibited output characters for the SASLprep profile."
  [codepoint]
  (or (non-ascii-space codepoint)
      (ascii-control codepoint)
      (non-ascii-control codepoint)
      (private-use codepoint)
      (non-character-code-point codepoint)
      (surrogate-code codepoint)
      (inappropriate-for-plain-text codepoint)
      (inappropriate-for-canonical-representation codepoint)
      (change-display-properties-or-deprecated codepoint)
      (tagging-character codepoint)))

(defn code-points-seq
  "Returns a lazy seq of the Unicode code points of the input string."
  [input]
  (lazy-seq
   (when (seq input)
     (let [codepoint (.codePointAt input 0)]
       (cons codepoint
             (code-points-seq
              (subs input (Character/charCount codepoint))))))))

(defn check-prohibited
  [input]
  (if (some saslprep-prohibited (code-points-seq input))
    (throw (error "Prohibited codepoints"))
    input))

(defn char-r-or-al
  [codepoint]
  (#{Character/DIRECTIONALITY_RIGHT_TO_LEFT
     Character/DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC}
   (Character/getDirectionality codepoint)))

(defn char-l
  [codepoint]
  (= Character/DIRECTIONALITY_LEFT_TO_RIGHT
     (Character/getDirectionality codepoint)))

(defn error
  [msg]
  (ex-info msg {:type ::normalization}))

(defn improper-bidi
  [input]
  (letfn [(run-state [{:keys [input-seq
                              contains-lcat
                              contains-randalcat
                              first?
                              first-randalcat?
                              last-randalcat?]
                       [codepoint & rest-codepoints] :input-seq}]
            (cond
              (and contains-randalcat
                   contains-lcat) (throw (error "Invalid bidirectionality"))
              (and (nil? codepoint)
                   contains-randalcat
                   (not (and first-randalcat?
                             last-randalcat?))) (throw
                                                 (error "Invalid bidirectionality"))
              (nil? codepoint) input

              (and first? (char-r-or-al codepoint)) (recur {:input-seq rest-codepoints
                                                            :contains-lcat contains-lcat
                                                            :contains-randalcat true
                                                            :first? false
                                                            :first-randalcat? true
                                                            :last-randalcat? last-randalcat?})
              (and (nil? rest-codepoints)
                   (char-r-or-al codepoint)) (recur {:input-seq rest-codepoints
                                                     :contains-lcat contains-lcat
                                                     :contains-randalcat true
                                                     :first? false
                                                     :first-randalcat? first-randalcat?
                                                     :last-randalcat? true})
              (char-r-or-al codepoint) (recur {:input-seq rest-codepoints
                                               :contains-lcat contains-lcat
                                               :contains-randalcat true
                                               :first? false
                                               :first-randalcat? first-randalcat?
                                               :last-randalcat? last-randalcat?})
              (char-l codepoint) (recur {:input-seq rest-codepoints
                                         :contains-lcat true
                                         :contains-randalcat contains-randalcat
                                         :first? false
                                         :first-randalcat? first-randalcat?
                                         :last-randalcat? last-randalcat?})
              :else (recur {:input-seq rest-codepoints
                            :contains-lcat contains-lcat
                            :contains-randalcat contains-randalcat
                            :first? false
                            :first-randalcat? first-randalcat?
                            :last-randalcat? last-randalcat?})))]
    (run-state {:input-seq (code-points-seq input)
                :contains-lcat false
                :contains-randalcat false
                :first? true
                :first-randalcat? false
                :last-randalcat? false})))

(defn saslprep
  "Prepares `input` using the SASLprep profile.
  See https://tools.ietf.org/html/rfc4013."
  [input]
  (->> input
       (map #(or (saslprep-mapping %) %))
       (filter #(not= ::nothing %))
       (apply str)
       (nfkc)
       (check-prohibited)
       (improper-bidi)))
