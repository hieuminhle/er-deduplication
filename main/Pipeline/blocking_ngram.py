import hashlib

def ngrams_string(s, n):
    return [s[i:i+n] for i in range(len(s)-n+1)]

# created this afterwards so we dont need to use the other initial_..._ngram methods (didnt delete the others for know, 
# because I used them for matching can be changed after we decide our baseline)
def initial_ngram(df, n, columns):
    if 'publication_venue' in columns:
        df ['publication_venue'] = df['publication_venue'].apply(lambda x: 'sigmod' if 'sigmod' in x else 'vldb')

    blocks = {}
    for index, row in df.iterrows():
        components_values = []

        for column in columns:
            if column == 'author_names':
                author_initials = []
                for name in row['author_names'].split(): 
                    parts = name.split()
                    if len(parts) > 1:
                        author_initials.append(parts[0][0] + parts[-1][0])
                    else:
                        author_initials.append(name[0])
                component_value = "".join(author_initials)
            elif column == 'paper_title':
                component_value = "".join(word[0] for word in row['paper_title'].split())
            elif column == 'publication_venue':
                component_value = row['publication_venue'] 
            elif column == 'year':
                component_value = str(row["year"]) 
        
            components_values.append(component_value)

        blocking_key = ''.join(components_values) 
        blocking_key_ngrams = list(ngrams_string(blocking_key, n))
        block_data = {'ngram_values': blocking_key_ngrams, 'index': [row['index']]}

        if blocking_key in blocks:
            unique_indices = list(set(blocks[blocking_key]['index'] + block_data['index'])) 
            blocks[blocking_key]['index'] = unique_indices
        else:
            blocks[blocking_key] = block_data
    return blocks

        

def ngrams_tuple(s, n):
    return [tuple(s[i:i+n]) for i in range(len(s)-n+1)]
# ignorieren 

def n_gram_blocking(df, n, columns):
    if 'publication_venue' in columns:
        df ['publication_venue'] = df['publication_venue'].apply(lambda x: 'sigmod' if 'sigmod' in x else 'vldb')
   
    if 'year' in columns:
        df['year'] = df['year'].astype(str)
    
    # n-gram for each selected column 
    for column in columns:
        key_name = f'ngram_key_{column}'
        df[key_name] = df[column].apply(lambda x: ''.join(map(str, x)))

        ngram_col_name = f'ngram_values_{column}'
        df[ngram_col_name] = df[key_name].apply(lambda x: tuple(ngrams_tuple(x, n)))

    # block them and each block consists of n-gram values and the respective index (id) 
    blocks = {}
    for index, row in df.iterrows():
        ngram_values = {column: row[f'ngram_values_{column}'] for column in columns}
        hashable_ngram_values = hashlib.sha256(str(hash(tuple(sorted(ngram_values.items())))).encode()).hexdigest()
        
        if hashable_ngram_values in blocks:
            blocks[hashable_ngram_values]['index'].add(row['index'])
        else:
            blocks[hashable_ngram_values] = {'index': {row['index']}, **ngram_values}

    return blocks
