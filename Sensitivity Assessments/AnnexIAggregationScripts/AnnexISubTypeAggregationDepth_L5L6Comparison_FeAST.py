########################################################################################################################

# Title: Annex I Sub Type Aggregation Depth - Comparison FeAST

# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk


########################################################################################################################

#                                               Aggregation Preparation:                                               #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required or data manipulation
import os
import time
import pandas as pd

#############################################################

# Load all data from the original Annex I Sub Type Aggregation Depth which only used L5 data
L5_only = pd.read_csv(r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\DepthAggregation\FeatureLevelAggregationDepth_V6L5_20200717.csv",)

# Load all data from the original Annex I Sub Type Aggregation Depth which comprises both L5 & L6 data
L6 = pd.read_csv(r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\DepthAggregation\FeatureLevelAggregationDepth_V6L6_20200717.csv")

#############################################################

# Remove any potentially erroneous whitespace from the data prior to merging
L5_only['Pressure'] = L5_only['Pressure'].str.strip()
L5_only['Annex I Habitat'] = L5_only['Annex I Habitat'].str.strip()
L5_only['Annex I sub-type'] = L5_only['Annex I sub-type'].str.strip()
L5_only['Depth zone'] = L5_only['Depth zone'].str.strip()

L6['Pressure'] = L6['Pressure'].str.strip()
L6['Annex I Habitat'] = L6['Annex I Habitat'].str.strip()
L6['Annex I sub-type'] = L6['Annex I sub-type'].str.strip()
L6['Depth zone'] = L6['Depth zone'].str.strip()

# Create merge between both L5_only and L6 DF's to enable comparison -- NEED TO FIX, DOES NOT WORK --
# L56 pairings not in 5 only
mergeDF = pd.merge(L5_only, L6, on=[
    'Pressure', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone'], how='outer', indicator=False)

# mergeDF = pd.merge(L5_only, L6, how='outer', on=[
#     'Unnamed: 0'], indicator=True)

# Rearrange columns to form order of interest
mergeDF = mergeDF[
    [
        'Unnamed: 0_x', 'Unnamed: 0_y', 'Pressure', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone',
        'AggregatedSensitivity_x', 'AggregatedSensitivity_y', 'AssessedCount_x', 'AssessedCount_y',
        'UnassessedCount_x', 'UnassessedCount_y'
    ]
]

# Rename columns within merDF
mergeDF.columns = [
    'Unnamed: 0_L5', 'Unnamed: 0_L6', 'Pressure', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone',
    'AggregatedSensitivity_L5', 'AggregatedSensitivity_L6', 'AssessedCount_L5', 'AssessedCount_L6',
    'UnassessedCount_L5', 'UnassessedCount_L6'
]


# Define function which iterates throuh two target columns and identifies if values are the same
def comparison(row, test_val):
    # Load all data for aggregated sensitivity scores
    agg_sens_L5 = row['AggregatedSensitivity_L5']
    agg_sens_L6 = row['AggregatedSensitivity_L6']
    # Load all data for assessed count scores
    ass_count_L5 = row['AssessedCount_L5']
    ass_count_L6 = row['AssessedCount_L6']
    # Load all data for unassessed count scores
    unass_count_L5 = row['UnassessedCount_L5']
    unass_count_L6 = row['UnassessedCount_L6']

    if 'AggSens' in test_val:
        if str(agg_sens_L5) == str(agg_sens_L6):
            return 'No change'
        else:
            return 'This value has changed'
    if 'AssCount' in test_val:
        if str(ass_count_L5) == str(ass_count_L6):
            return 'No change'
        else:
            return 'This value has changed'
    if 'UnCount' in test_val:
        if str(unass_count_L5) == str(unass_count_L6):
            return 'No change'
        else:
            return 'This value has changed'


# Run comparison function on the AggSens setting:
mergeDF['AggregatedSensitivityComparison'] = mergeDF.apply(lambda row: comparison(row, 'AggSens'), axis=1)

# Run comparison function on the AggCount setting:
mergeDF['AssessedCountComparison'] = mergeDF.apply(lambda row: comparison(row, 'AssCount'), axis=1)

# Run comparison function on the UnCount setting:
mergeDF['UnassessedCountComparison'] = mergeDF.apply(lambda row: comparison(row, 'UnCount'), axis=1)

# Rearrange columns to create comparison output DF
mergeDF = mergeDF[
    [
        'Pressure', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone', 'AggregatedSensitivity_L5',
        'AggregatedSensitivity_L6', 'AssessedCount_L5', 'AssessedCount_L6', 'UnassessedCount_L5',
        'UnassessedCount_L6',
        'AggregatedSensitivityComparison', 'AssessedCountComparison', 'UnassessedCountComparison'
    ]
]

mergeDF = mergeDF[
    [
        'Pressure', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone', 'AggregatedSensitivity_L5',
        'AggregatedSensitivity_L6', 'AggregatedSensitivityComparison', 'AssessedCount_L5', 'AssessedCount_L6',
        'AssessedCountComparison', 'UnassessedCount_L5', 'UnassessedCount_L6', 'UnassessedCountComparison',

    ]
]

# Create output for QC and analysis in Excel
mergeDF.to_csv(r'Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\DepthAggregation\AnnexISubTypeAggregationDepth_L5_L6Comparison.csv', sep=',')