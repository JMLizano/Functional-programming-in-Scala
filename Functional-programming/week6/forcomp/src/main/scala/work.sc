import forcomp.Anagrams
import forcomp.Anagrams.sentenceAnagrams


val occ = Anagrams.wordOccurrences("holaaa")
val comb = Anagrams.combinations(occ)

val occurrences = List(('a',4), ('l',1), ('h',1), ('o',1))
val combs = Anagrams.combinations(occurrences)
combs.length
val comb2 = combs.filter(l => {
  val l2 = l.map(item => item._1)
  l2.distinct.size  == l2.size
})

comb2.drop(7).foreach(println)

val sentence = List("Linux", "rulez")
sentenceAnagrams(sentence)


















