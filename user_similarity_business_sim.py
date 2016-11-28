###
###
# Author Info:
#     This code is modified from code originally written by Jim Blomo and Derek Kuo
##/


from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep


class UserSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1_extract_user_business(self,_,record):
        """Taken a record, yield <user_id, business_id>"""
        yield [record['user_id'], record['business_id']]

    def reducer1_compile_businesses_under_user(self,user_id,business_ids):
        ###
        # TODO_1: compile businesses as a list of array under given user_id,after remove duplicate business, yield <user_id, [business_ids]>
        ##/
        unique_business_ids = set(business_ids)
        yield user_id, list(unique_business_ids)

    def mapper2_collect_businesses_under_user(self, user_id, business_ids):
        ###
        # TODO_2: collect all <user_id, business_ids> pair, map into the same Keyword LIST, yield <'LIST',[user_id, [business_ids]]>
        ##/
        yield 'LIST', [user_id, business_ids]

    def reducer2_calculate_similarity(self,stat,user_business_ids):
        def Jaccard_similarity(business_list1, business_list2):
            ###
            # TODO_3: Implement Jaccard Similarity here, output score should between 0 to 1
            ##/
            union = set(business_list1 + business_list2)
            #intersection = [entry for entry in list(union) if entry in business_list1 
            #    and entry in business_list2]
            intersection = [entry for entry in business_list1 if entry in business_list2]
            return len(intersection) / len(list(union))

        ###
        # TODO_4: Calulate Jaccard, output the pair users that have similarity over 0.5, yield <[user1,user2], similarity>
        ##/
        # make a for loop that grabs every 2 entries in the list
        user_business_ids_lst = list(user_business_ids)
        num_of_users = len(user_business_ids_lst)
        # user_index = 0
        # while (user_index < num_of_users - 1):
        #     js = Jaccard_similarity(user_business_ids_lst[user_index][1][0], 
        #         user_business_ids_lst[user_index + 1][1][0])
        #     user1 = user_business_ids_lst[user_index]
        #     user2 = user_business_ids_lst[user_index + 1]
        #     if js > 0.5:
        #         yield [user1, user2], js
        for i in range(num_of_users):
            for j in range(i + 1, num_of_users):
                js = Jaccard_similarity(user_business_ids_lst[i][1], 
                    user_business_ids_lst[j][1]) 
                if js >= 0.5:
                    user1 = user_business_ids_lst[i][0]
                    user2 = user_business_ids_lst[j][0]
                    yield [user1, user2], js
                #pass
        #yield stat, user_business_ids_lst[0]
        #for user_and_business in user_business_ids:

        #if Jaccard_similarity(user_business_ids[1]) > 0.5:
        #    yield user_business_ids[0], stat
        #for user, business_id_lst in user_business_ids:
        #yield []

    def steps(self):
        return [
            MRStep(mapper=self.mapper1_extract_user_business, reducer=self.reducer1_compile_businesses_under_user),
            MRStep(mapper=self.mapper2_collect_businesses_under_user, reducer= self.reducer2_calculate_similarity)
        ]


if __name__ == '__main__':
    UserSimilarity.run()
