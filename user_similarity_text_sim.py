###
###
# Author Info:
#     This code is modified from code originally written by Jim Blomo and Derek Kuo
##/


from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep

import re

WORD_RE = re.compile(r"[\w']+")

class UserSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1_extract_user_words(self,_,record):
        """Taken a record, yield <user_id, business_id>"""
        #yield [record['user_id'], record['business_id']]
        for word in WORD_RE.findall(record['text']):
            yield [ record['user_id'], word.lower() ]

    def reducer1_compile_words_under_user(self,user_id,words):
        ###
        # TODO_1: compile businesses as a list of array under given user_id,after remove duplicate words, yield <user_id, [words]>
        ##/
        unique_words = set(words)
        yield user_id, list(unique_words)

    def mapper2_collect_words_under_user(self, user_id, words):
        ###
        # TODO_2: collect all <user_id, words> pair, map into the same Keyword LIST, yield <'LIST',[user_id, [words]]>
        ##/
        yield 'LIST', [user_id, words]

    def reducer2_calculate_similarity(self,stat,user_words):
        def Jaccard_similarity(word_list1, word_list2):
            ###
            # TODO_3: Implement Jaccard Similarity here, output score should between 0 to 1
            ##/
            #union = set(word_list1 + word_list2)
            union = set(word_list1).union(word_list2)
            intersection = list( set(word_list1) & set(word_list1) ) #[entry for entry in word_list1 if entry in word_list2]
            return len(intersection) / len(list(union))

        ###
        # TODO_4: Calculate Jaccard, output the pair users that have similarity over 0.5, yield <[user1,user2], similarity>
        ##/
        user_words_lst = list(user_words)
        num_of_users = len(user_words_lst)

        for i in range(num_of_users):
            for j in range(i + 1, num_of_users):
                js = Jaccard_similarity(user_words_lst[i][1], 
                    user_words_lst[j][1]) 
                if js >= 0.5:
                    user1 = user_words_lst[i][0]
                    user2 = user_words_lst[j][0]
                    yield [user1, user2], js
        #yield stat, user_words_lst[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper1_extract_user_words, reducer=self.reducer1_compile_words_under_user),
            MRStep(mapper=self.mapper2_collect_words_under_user, reducer= self.reducer2_calculate_similarity)
        ]


if __name__ == '__main__':
    UserSimilarity.run()
