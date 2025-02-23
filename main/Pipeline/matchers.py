import time


# apply similarity to not blocked dataframes and get the baseline / ground truth
def apply_similarity_baseline(df1, df2, threshold, selected_columns, similarity_function):
    similar_pairs = []

    for index1, row1 in df1.iterrows():
        for index2, row2 in df2.iterrows():
            average_similarity = 0.0

            for col_df1, col_df2 in zip(selected_columns, selected_columns):
                value_df1 = set(row1[col_df1].lower().split())
                value_df2 = set(row2[col_df2].lower().split())

                similarity = similarity_function(value_df1, value_df2)
                average_similarity += similarity

            if len(selected_columns) > 1:
                average_similarity /= len(selected_columns)
            if average_similarity >= threshold:

                pair = (row1['index'], row2['index'])
                similar_pairs.append(pair)
    return similar_pairs
indices = ['author_names', 'paper_title']
# this method compares each block of blocks1 against each block of blocks2
def apply_similarity_blocks(blocks1, blocks2, threshold, similarity_function, indices):
    similar_pairs = []

    # Record the start time
    start_time = time.time()

    for key1, block1 in blocks1.items():
        for key2, block2 in blocks2.items():
            average_similarity = 0.0

            value_block1 = [block1.get(i, '') for i in indices]
            value_block2 = [block2.get(i, '') for i in indices]
            similarity = similarity_function(value_block1, value_block2)

            average_similarity += similarity

            if len(indices) > 1:
                average_similarity /= len(indices)

            if average_similarity >= threshold:
                index_pairs = [(i, j) for i in block1['index'] for j in block2['index']]
                similar_pairs.extend(index_pairs)

    # Record the end time
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the elapsed time
    print(f"Processing time: {elapsed_time} seconds. Number of similar pairs: {len(similar_pairs)}")

    return similar_pairs

# this method is for sorted blockings where we match respective blocks lets say we block the datframe by years so 
# for each year a block. The Idea is to match then only block with 1995 against block with 1995 etc
def apply_similarity_sorted(blocks1, blocks2, threshold, similarity_function, indices):
    similar_pairs = [] 

    start_time = time.time() 

    for block1, block2 in zip(blocks1, blocks2):
        for elem1 in block1:
            for elem2 in block2:
                average_similarity = 0.0

                for index in indices:
                    value_elem1 = elem1.get(index, '')
                    value_elem2 = elem2.get(index, '')

                    similarity = similarity_function(value_elem1, value_elem2)
                    average_similarity += similarity

                if len(indices) > 1:
                    average_similarity /= len(indices)

                if average_similarity >= threshold:
                    index_pair = (elem1['index'], elem2['index'])
                    similar_pairs.append(index_pair)

    end_time = time.time()  # Record the ending time for performance measurement
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    print(f"Processing time: {elapsed_time} seconds. Number of similar pairs: {len(similar_pairs)}")

    return similar_pairs  # Return the list of pairs of indices that meet the similarity threshold

# blocks1 = Liste und die Liste enthält listen mit einträgen als Dictionary


# this is baiscally like sorthed, but the idea is that I sort first with sorted than each creatd block
# is used for example n-gram blocking which gives dictionariex 
def apply_similarity_sorted_dictionary(blocks1, blocks2, threshold, similarity_function, indices):
    similar_pairs = []

    start_time = time.time()

    for (key1, block1), (key2, block2) in zip(blocks1.items(), blocks2.items()):
        for elem1 in block1[indices]:
            for elem2 in block2[indices]:
                average_similarity = 0.0
                similarity = similarity_function(elem1, elem2)
                average_similarity += similarity

                if average_similarity >= threshold:
                    index_pair = (block1['index'][0], block2['index'][0])  # Assuming there is only one index per block
                    similar_pairs.append(index_pair)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing time: {elapsed_time} seconds. Number of similar pairs: {len(similar_pairs)}")

    return similar_pairs

# matcher for compare lengths
def apply_similarity_lengths(blocks1, blocks2, threshold, similarity_function, indices):
    similar_pairs = []

    # Record the start time
    start_time = time.time()

    for key1, block1 in blocks1.items():
        for key2, block2 in blocks2.items():
            average_similarity = 0.0

            value_block1 = [block1.get(indices)]
            value_block2 = [block2.get(indices)]
            similarity = similarity_function(value_block1, value_block2)

            if similarity >= threshold:
                index_pairs = [(i, j) for i in block1['index'] for j in block2['index']]
                similar_pairs.extend(index_pairs)

    # Record the end time
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the elapsed time
    print(f"Processing time: {elapsed_time} seconds. Number of similar pairs: {len(similar_pairs)}")

    return similar_pairs
