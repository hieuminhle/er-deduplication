import pandas as pd

# blocks by year
def block_by_year_ranges(df, year_block, labels):
    year_ranges = {}
    
    df["year_range"] = pd.cut(df['year'], bins=year_block, labels=labels, right=False)
    
    for label in labels:
        intervals = df[df["year_range"] == label].to_dict(orient='records')
        year_ranges[label] = intervals

    return year_ranges


# blocks by year and publisher -> for year 1995 two blocks one with only sigmod and one with only vldb
def block_by_year_and_publisher(df, year_block, labels):
    year_ranges = []
    publishers = ['sigmod', 'vldb']  

    for label in labels:
        publisher_blocks = []
        intervals = df[(df['year'] >= year_block[labels.index(label)]) & (df['year'] < year_block[labels.index(label) + 1])]

        for publisher in publishers:
            publisher_block = intervals[intervals["publication_venue"].str.contains(publisher)]
            if not publisher_block.empty:
                publisher_blocks.append(publisher_block.to_dict(orient='records'))

        year_ranges.extend(publisher_blocks)

    return year_ranges
#

"""
def block_by_year_ranges_key_dynamic2(df, year_block, labels):
    year_ranges = []
    df["year_range"] = pd.cut(df['year'], bins=year_block, labels=labels, right=False)
    publishers = ['sigmod', 'vldb']  # List of publishers to separate data based on

    for label in labels:
        publisher_blocks = []
        intervals = df[df["year_range"] == label]

        for publisher in publishers:
            publisher_block = intervals[intervals["publication_venue"].str.contains(publisher)]
            if publisher_block.size > 0:
                # publisher_block.iterrows() for  all columns?
                publisher_blocks.append(publisher_block)

        year_ranges.extend(publisher_blocks)

    return year_ranges



def block_by_year_dynamic(df, year_input, labels):
    year_ranges = []
    publishers = ['sigmod', 'vldb']  # List of publishers to separate data based on

    if isinstance(year_input, int):  # Single year
        intervals = df[df['year'] == year_input]
    elif isinstance(year_input, list) and len(year_input) > 1:  # Year range
        intervals = df[df['year'].between(year_input[0], year_input[1], inclusive='left')]
    
    for label in labels:
        publisher_blocks = []
        for publisher in publishers:
            publisher_block = intervals[intervals["publication_venue"].str.contains(publisher)]
            if publisher_block.size > 0:
                publisher_blocks.append(publisher_block)

        year_ranges.extend(publisher_blocks)

    return year_ranges
"""
