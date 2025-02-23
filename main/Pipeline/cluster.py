def build_clusters(matched_entities):
    clusters = []

    for pair in matched_entities:
        entity1, entity2 = pair
        found_clusters = []
        for cluster in clusters:
            if entity1 in cluster or entity2 in cluster:
                found_clusters.append(cluster)

        if not found_clusters:
            clusters.append(set(pair))
        elif len(found_clusters) == 1:
            found_clusters[0].update(pair)
        else:
            merged_cluster = set.union(*found_clusters)
            clusters = [c for c in clusters if c not in found_clusters]
            clusters.append(merged_cluster)

    return clusters

def clustering_matches(matched_entities):
    clusters = []

    for pair in matched_entities:
        entity1, entity2 = pair
        merged_cluster = None

        for cluster in clusters:
            if any(value in cluster for value in pair):
                cluster.update(pair)
                merged_cluster = cluster
                break
        if merged_cluster is None:
            clusters.append(set(pair))

    return clusters


