def length_blocking_multi_columns_named(df, fields):
    blocks = {}
    for index, row in df.iterrows():
        lengths = tuple(len(row[field]) for field in fields)
        key_name = f"Lengths_{'_'.join(map(str, lengths))}" 
        if key_name not in blocks:
            blocks[key_name] = {'lengths': lengths, 'index': []}
        blocks[key_name]['index'].append(row['index'])
    return blocks

