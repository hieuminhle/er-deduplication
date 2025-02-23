import hashlib

def initial_hash(df, columns):
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
        blocking_key_hash = hashlib.md5(blocking_key.encode()).hexdigest()
        block_data = {'hash_value': blocking_key_hash, 'index': [row['index']]}
        if blocking_key in blocks:
            unique_indices = list(set(blocks[blocking_key]['index'] + block_data['index']))
            blocks[blocking_key]['index'] = unique_indices
        else:
            blocks[blocking_key] = block_data

    return blocks


def hash_blocking(dataframe, columns):
    #if 'publication_venue' in blocking_columns:
     #   dataframe ['publication_venue'] = blocking_columns['publication_venue'].apply(lambda x: 'sigmod' if 'sigmod' in x else 'vldb')
    dataframe['hash_key'] = dataframe[columns].astype(str).agg(''.join, axis=1)
    dataframe['hash_value'] = dataframe['hash_key'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    blocks = dataframe.groupby('hash_value').apply(lambda x: {'hash_value': x['hash_value'].iloc[0], 'index': x['index'].tolist()}).to_dict()
    return blocks


