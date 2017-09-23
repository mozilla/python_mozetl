"""
Bug 1386274 - TAAR similarity-based add-on donor list

This job clusters users into different groups based on their
active add-ons. A representative users sample is selected from
each cluster ("donors") and is saved to a model file along
with a feature vector that will be used, by the TAAR library
module, to perform recommendations.
"""

import click
import json
import logging
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml import Pipeline
from pyspark.mllib.stat import KernelDensity
from scipy.spatial import distance
from taar_utils import store_json_to_s3, load_amo_external_whitelist

# Define the set of feature names to be used in the donor computations.
CATEGORICAL_FEATURES = ["geo_city", "locale", "os"]
CONTINUOUS_FEATURES = \
    ["subsession_length", "bookmark_count", "tab_open_count", "total_uri", "unique_tlds"]
# Set a constrain on the pairwise comparisons that are allowed
#  both at an intra- and inter-custer level to avoid OOM errors in KDE.
KDE_SIZE_THRESH = 1000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_samples(spark):
    """ Get a DataFrame with a valid set of sample to base the next
    processing on.
    """
    return (
        spark.sql("SELECT * FROM longitudinal")
        .where("active_addons IS NOT null")
        .where("size(active_addons[0]) > 2")
        .where("size(active_addons[0]) < 100")
        .where("normalized_channel = 'release'")
        .where("build IS NOT NULL AND build[0].application_name = 'Firefox'")
        .selectExpr(
            "client_id as client_id",
            "active_addons[0] as active_addons",
            "geo_city[0] as geo_city",
            "subsession_length[0] as subsession_length",
            "settings[0].locale as locale",
            "os as os",
            "places_bookmarks_count[0].sum AS bookmark_count",
            "scalar_parent_browser_engagement_tab_open_event_count[0].value AS tab_open_count",
            "scalar_parent_browser_engagement_total_uri_count[0].value AS total_uri",
            "scalar_parent_browser_engagement_unique_domains_count[0].value AS unique_tlds"
        )
    )


def get_addons_per_client(users_df, addon_whitelist, minimum_addons_count):
    """ Extracts a DataFrame that contains one row
    for each client along with the list of active add-on GUIDs.
    """
    def is_valid_addon(guid, addon):
        return not (
            addon.is_system or
            addon.app_disabled or
            addon.type != "extension" or
            addon.user_disabled or
            addon.foreign_install or
            guid not in addon_whitelist
        )

    # Create an add-ons dataset un-nesting the add-on map from each
    # user to a list of add-on GUIDs. Also filter undesired add-ons.
    return (
        users_df.rdd
        .map(lambda p: (p["client_id"],
             [guid for guid, data in p["active_addons"].items() if is_valid_addon(guid, data)]))
        .filter(lambda p: len(p[1]) > minimum_addons_count)
        .toDF(["client_id", "addon_ids"])
    )


def compute_clusters(addons_df, num_clusters, random_seed):
    """ Performs user clustering by using add-on ids as features.
    """

    # Build the stages of the pipeline. We need hashing to make the next
    # steps work.
    hashing_stage = HashingTF(inputCol="addon_ids", outputCol="hashed_features")
    idf_stage = IDF(inputCol="hashed_features", outputCol="features", minDocFreq=1)
    # As a future improvement, we may add a sane value for the minimum cluster size
    # to BisectingKMeans (e.g. minDivisibleClusterSize). For now, just make sure
    # to pass along the random seed if needed for tests.
    kmeans_kwargs = {"seed": random_seed} if random_seed else {}
    bkmeans_stage = BisectingKMeans(k=num_clusters, **kmeans_kwargs)
    pipeline = Pipeline(stages=[hashing_stage, idf_stage, bkmeans_stage])

    # Run the pipeline and compute the results.
    model = pipeline.fit(addons_df)
    return (
        model
        .transform(addons_df)
        .select(["client_id", "prediction"])
    )


def get_donor_pools(users_df, clusters_df, num_donors, random_seed=None):
    """ Samples users from each cluster.
    """
    cluster_population = clusters_df.groupBy("prediction").count().collect()
    clusters_histogram =\
        [(x["prediction"], x["count"]) for x in cluster_population]

    # Sort in-place from highest to lowest populated cluster.
    clusters_histogram.sort(key=lambda x: x[0], reverse=False)

    # Save the cluster ids and their respective scores separately.
    clusters = [cluster_id for cluster_id, _ in clusters_histogram]
    counts = [donor_count for _, donor_count in clusters_histogram]

    # Compute the proportion of user in each cluster.
    total_donors_in_clusters = sum(counts)
    clust_sample = [float(t) / total_donors_in_clusters for t in counts]
    sampling_proportions = dict(zip(clusters, clust_sample))

    # Sample the users in each cluster according to the proportions
    # and pass along the random seed if needed for tests.
    sampling_kwargs = {"seed": random_seed} if random_seed else {}
    donor_df = clusters_df.sampleBy("prediction",
                                    fractions=sampling_proportions,
                                    **sampling_kwargs)
    # Get the specific number of donors for each cluster and drop the
    # predicted cluster number information.
    current_sample_size = donor_df.count()
    donor_pool_df = (
        donor_df
        .sample(False, float(num_donors) / current_sample_size, **sampling_kwargs)
    )
    return clusters, donor_pool_df


def get_donors(spark, num_clusters, num_donors, addon_whitelist, random_seed=None):
    # Get the data for the potential add-on donors.
    users_sample = get_samples(spark)
    # Get add-ons from selected users and make sure they are
    # useful for making a recommendation.
    addons_df = get_addons_per_client(users_sample, addon_whitelist, 2)
    # Perform clustering by using the add-on info.
    clusters = compute_clusters(addons_df, num_clusters, random_seed)
    # Sample representative ("donors") users from each cluster.
    cluster_ids, donors_df =\
        get_donor_pools(users_sample, clusters, num_donors, random_seed)

    # Finally, get the feature vectors for users that represent
    # each cluster. Since the "active_addons" in "users_sample"
    # are in a |MapType| and contain system add-ons as well, just
    # use the cleaned up list from "addons_df".
    return cluster_ids, (
        users_sample
        .join(donors_df, 'client_id')
        .drop('active_addons')
        .join(addons_df, 'client_id', 'left')
        .drop('client_id')
        .withColumnRenamed('addon_ids', 'active_addons')
    )


def format_donors_dictionary(donors_df):
    cleaned_records = donors_df.drop('prediction').collect()
    # Convert each row to a dictionary.
    return [row.asDict() for row in cleaned_records]


def similarity_function(x, y):
    """ Similarity function for comparing user features.

    This actually really should be implemented in taar.similarity_recommender
    and then imported here for consistency.
    """

    def safe_get(field, row, default_value):
        # Safely get a value from the Row. If the value is None, get the
        # default value.
        return row[field] if row[field] is not None else default_value

    # Extract the values for the categorical and continuous features for both
    # the x and y samples. Use an empty string as the default value for missing
    # categorical fields and 0 for the continuous ones.
    x_categorical_features = [safe_get(k, x, "") for k in CATEGORICAL_FEATURES]
    x_continuous_features = [safe_get(k, x, 0) for k in CONTINUOUS_FEATURES]
    y_categorical_features = [safe_get(k, y, "") for k in CATEGORICAL_FEATURES]
    y_continuous_features = [safe_get(k, y, 0) for k in CONTINUOUS_FEATURES]

    # Here a larger distance indicates a poorer match between categorical variables.
    j_d = (distance.hamming(x_categorical_features, y_categorical_features))
    j_c = (distance.canberra(x_continuous_features, y_continuous_features))

    # Take the product of similarities to attain a univariate similarity score.
    # Add a minimal constant to prevent zero values from categorical features.
    return abs((j_c + 0.001) * j_d)


def generate_non_cartesian_pairs(first_rdd, second_rdd, random_seed=None):
    # Add an index to all the elements in each RDD.
    rdd1_with_indices = first_rdd.zipWithIndex().map(lambda p: (p[1], p[0]))
    rdd2_with_indices = second_rdd.zipWithIndex().map(lambda p: (p[1], p[0]))
    # Join the RDDs using the indices as keys, then strip
    # them off before returning an RDD like [<v1, v2>, ...]
    rdd_large_out = rdd1_with_indices.join(rdd2_with_indices).map(lambda p: p[1])
    size_pair_rdd = rdd_large_out.count()
    if size_pair_rdd > KDE_SIZE_THRESH:
        return rdd_large_out.sample(False, KDE_SIZE_THRESH/size_pair_rdd, random_seed)
    else:
        return rdd_large_out


def get_lr_curves(spark, features_df, cluster_ids, kernel_bandwidth,
                  num_pdf_points, random_seed=None):
    """ Compute the likelihood ratio curves for clustered clients.

    Work-flow followed in this function is as follows:

     * Access the DataFrame including cluster numbers and features.
     * Load same similarity function that will be used in TAAR module.
     * Iterate through each cluster and compute in-cluster similarity.
     * Iterate through each cluster and compute out-cluster similarity.
     * Compute the kernel density estimate (KDE) per similarity score.
     * Linearly down-sample both PDFs to 1000 points.

    :param spark: the SparkSession object.
    :param features_df: the DataFrame containing the user features (e.g. the
                        ones coming from |get_donors|).
    :param cluster_ids: the list of cluster ids (e.g. the one coming from |get_donors|).
    :param kernel_bandwidth: the kernel bandwidth used to estimate the kernel densities.
    :param num_pdf_points: the number of points to sample for the LR-curves.
    :param random_seed: the provided random seed (fixed in tests).
    :return: A list in the following format
        [(idx, (lr-numerator-for-idx, lr-denominator-for-idx)), (...), ...]
    """

    # Instantiate holder lists for inter- and intra-cluster scores.
    same_cluster_scores_rdd = spark.sparkContext.emptyRDD()
    different_clusters_scores_rdd = spark.sparkContext.emptyRDD()

    random_split_kwargs = {"seed": random_seed} if random_seed else {}

    for cluster_number in cluster_ids:
        # Pick the features for users belonging to the current cluster.
        current_cluster_df = (
            features_df
            .where(col("prediction") == cluster_number)
        )
        # Pick the features for users belonging to all the other clusters.
        other_clusters_df = (
            features_df
            .where(col("prediction") != cluster_number)
        )

        logger.debug("Computing scores for cluster", extra={"cluster_id": cluster_number})

        # Compares the similarity score between pairs of clients in the same cluster.
        cluster_half_1, cluster_half_2 =\
            current_cluster_df.rdd.randomSplit([0.5, 0.5], **random_split_kwargs)
        pair_rdd = generate_non_cartesian_pairs(cluster_half_1, cluster_half_2)
        intra_scores_rdd = pair_rdd.map(lambda r: similarity_function(*r))
        same_cluster_scores_rdd = same_cluster_scores_rdd.union(intra_scores_rdd)

        # Compares the similarity score between pairs of clients in different clusters.
        pair_rdd =\
            generate_non_cartesian_pairs(current_cluster_df.rdd, other_clusters_df.rdd)
        inter_scores_rdd = pair_rdd.map(lambda r: similarity_function(*r))
        different_clusters_scores_rdd =\
            different_clusters_scores_rdd.union(inter_scores_rdd)

    # Determine a range of observed similarity values linearly spaced.
    all_scores_rdd = same_cluster_scores_rdd.union(different_clusters_scores_rdd)
    min_similarity = all_scores_rdd.min()
    max_similarity = all_scores_rdd.max()
    lr_index = np.arange(min_similarity, max_similarity,
                         float(abs(min_similarity - max_similarity)) / num_pdf_points)

    # Kernel density estimate for the inter-cluster comparison scores.
    kd_dc = KernelDensity()
    kd_dc.setSample(different_clusters_scores_rdd)
    kd_dc.setBandwidth(kernel_bandwidth)
    denominator_density = kd_dc.estimate(lr_index)

    # Kernel density estimate for the intra-cluster comparison scores.
    kd_sc = KernelDensity()
    kd_sc.setSample(same_cluster_scores_rdd)
    kd_sc.setBandwidth(kernel_bandwidth)
    numerator_density = kd_sc.estimate(lr_index)

    # Structure this in the correct output format.
    return zip(lr_index, zip(numerator_density, denominator_density))


@click.command()
@click.option('--date', required=True)
@click.option('--bucket', default='telemetry-private-analysis-2')
@click.option('--prefix', default='taar/similarity/')
@click.option('--num_clusters', default=20)
@click.option('--num_donors', default=1000)
@click.option('--kernel_bandwidth', default=0.35)
@click.option('--num_pdf_points', default=1000)
def main(date, bucket, prefix, num_clusters, num_donors, kernel_bandwidth, num_pdf_points):
    spark = (SparkSession
             .builder
             .appName("taar_similarity")
             .enableHiveSupport()
             .getOrCreate())

    if num_donors < 100:
        logger.warn("Less than 100 donors were requested.", extra={"donors": num_donors})
        num_donors = 100

    logger.info("Loading the AMO whitelist...")
    whitelist = load_amo_external_whitelist()

    logger.info("Computing the list of donors...")

    # Compute the donors clusters and the LR curves.
    cluster_ids, donors_df = get_donors(spark, num_clusters, num_donors, whitelist)
    lr_curves = get_lr_curves(spark, donors_df, cluster_ids, kernel_bandwidth,
                              num_pdf_points)

    # Store them.
    donors = format_donors_dictionary(donors_df)
    store_json_to_s3(json.dumps(donors, indent=2), 'donors',
                     date, prefix, bucket)
    store_json_to_s3(json.dumps(lr_curves, indent=2), 'lr_curves',
                     date, prefix, bucket)
    spark.stop()
