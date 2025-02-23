import numpy as np

def levensthein_distance(set1, set2):
    # make the subcriptable 
    list1, list2 = list(set1), list(set2)
    m, n = len(list1), len(list2)

    # create matrix
    matrix = np.zeros((m + 1, n + 1))

    # fill row with length of string 
    for i in range(1, m + 1):
        matrix[i][0] = i

    # fill column with lenth of string 
    for j in range(1, n + 1):
        matrix[0][j] = j
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if list1[i - 1] ==  list2[j - 1]:
                cost = 0
            else:
                cost = 1
            
            matrix[i][j] = min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j - 1] + cost)
    similarity = 1 - (matrix[m][n] / (m + n))
    return similarity

def jaccard_similarity(set1, set2):
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    return intersection / union if union > 0 else 0

def jaccard_similarity_ngrams(ngrams1, ngrams2):
    set1 = set(map(tuple, ngrams1))
    set2 = set(map(tuple, ngrams2))

    intersection = len(set1.intersection(set2))
    union = len(set1) + len(set2) - intersection   

    if union == 0:
        return 0.0  
    
    return intersection / union


# as in lecture for trigram example
def n_gram_similarity(df1, df2):
    set_df1 = {item for sublist in df1 for item in sublist}
    set_df2 = {item for sublist in df2 for item in sublist}
    intersection = len(set_df1 & set_df2)
    add = (len(set_df1) + len(set_df2))  
    return 2 * intersection / add if add > 0 else 0

# basically same but handles different data structure then n_gram_similarity 
def n_gram_similarity2(df1, df2):
    df1, df2 = set(df1), set(df2)
    intersection = len(df1 & df2)
    return 2 * intersection / len(df1) + len(df2)

def exact_length_similarity(lengths1, lengths2):
    # Check if lengths are exactly the same
    if lengths1 == lengths2:
        return 1.0  # Full similarity for exact match
    else:
        return 0.0

