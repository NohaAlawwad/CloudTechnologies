from __future__ import division, unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
from math import log
import re
import itertools
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

WORD_RE = re.compile(r"[\w']+")

class MRTFIDF(MRJob):

    # output => (word, docName, D), 1
    def mapper_get_words(self, _, csv_doc):
        lst = csv_doc.split(',')
        userid = lst[0]
        body = lst[1]

        users_number = 0
        for user_owner in lst:
            user_owner = lst[0]
            users_number = (users_number + 1)

        d = users_number

        for word in WORD_RE.findall(body):
            yield (word.lower(), userid ,d), 1

    # (word, docName, d), n
    def reducer_count_words_per_doc(self, docInfo, occurences):
         yield (docInfo[0], docInfo[1], docInfo[2]), sum(occurences)

    # => docname, (word,n,d)
    def mapper_total_number_of_words_per_docs(self, docInfo, n):
         yield docInfo[1], (docInfo[0],n,docInfo[2])

    # compute the total number of terms in each doc =>(word,docName, d), (n,N)
    def reducer_total_number_of_words_per_docs(self,docName,words_per_doc):
        total = 0
        n = []
        word = []
        d = []
        for value in words_per_doc:
            total += value[1]
            n.append(value[1])
            word.append(value[0])
            d.append(value[2])
            #N1 = total*len(word)
            #N.append(N1)

        N = [total]*len(word)

        for value in range(len(word)):
            yield (word[value], docName,d[value]), (n[value], N[value])

    def mapper_number_of_documents_a_word_appear_in(self, wordInfo, wordCounts):
        yield wordInfo[0], (wordInfo[1], wordCounts[0], wordCounts[1],wordInfo[2],1)

    # number of documents in the corpus in which the word appears
    # => ((word, docname, D), (n, N, m))
    def reducer_word_frequency_in_corpus(self, word, wordInfoAndCounts):
        total = 0
        docName = []
        n = []
        N = []
        d = []
        for value in wordInfoAndCounts:
            total += 1
            docName.append(value[0])
            n.append(value[1])
            N.append(value[2])
            d.append(value[3])
        # we need to compute the total numbers of documents in corpus , m is the number of doc contain a specified  word

        m = [total]*len(n)

        for value in range(len(m)):
             yield (word, docName[value],d[value]), (n[value], N[value], m[value])

    # compute tf-idf
    # ((word, docname, D), (n, N, m)) => ((word, docname), TF*IDF)
    def mapper_calculate_tf_idf(self, wordInfo, wordMetrics):
        tfidf = (wordMetrics[0] / wordMetrics[1]) * log(1+(wordInfo[2]/wordMetrics[2]))
        print (wordInfo[0]+','+wordInfo[1]+','+str(tfidf))
        #yield (wordInfo[0],wordInfo[1]), tfidf

    def steps(self):
        return [
             MRStep(mapper=self.mapper_get_words,
                    reducer=self.reducer_count_words_per_doc),
             MRStep(mapper=self.mapper_total_number_of_words_per_docs,
                    reducer=self.reducer_total_number_of_words_per_docs),
             MRStep(mapper=self.mapper_number_of_documents_a_word_appear_in,
                    reducer=self.reducer_word_frequency_in_corpus),
             MRStep(mapper=self.mapper_calculate_tf_idf)
        ]


if __name__ == '__main__':
    MRTFIDF.run()
