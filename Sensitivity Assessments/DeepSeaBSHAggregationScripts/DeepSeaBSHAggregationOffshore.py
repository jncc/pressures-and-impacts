########################################################################################################################

# Title: Habitats of Conservation Importance (HOCI) Aggregation

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
import numpy as np
import pandas as pd

#############################################################

# Load in all BSH data from MS xlsx document
bsh = pd.read_excel(r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\DeepSea_BSH.xlsx", 'BSH_Biotope_PresenceAbsence')


# Load all MarESA sensitivity assessment data
# Define a directory to be searched
maresa_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Aggregation_InputData\MarESAExtract"
# Set this as the working directory
os.chdir(maresa_dir)
# Read in all files within this target directory
maresa_files = filter(os.path.isfile, os.listdir(maresa_dir))
# Add full filepaths to the identified files within said directory
maresa_files = [os.path.join(maresa_dir, f) for f in maresa_files]
# Sort all files read in by the most recently edited first
maresa_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

# Define the MarESA object with unknowns - read the last edited file
maresa = pd.read_csv(maresa_files[0], dtype={'JNCC_Code': str})

#############################################################

# Clean all data and merge together

# Strip all trailing whitespace from both the annex1 DF and the maresa DF prior to merging

bsh['JNCC code'] = bsh['JNCC code'].str.strip()
maresa['JNCC_Code'] = maresa['JNCC_Code'].str.strip()

# Refine bsh to only include yes and posible
bsh = bsh[bsh['Present in Canyons MCZ?'] != 'No']
bsh = bsh[bsh['Present in Canyons MCZ?'] != 'Unlikely']

# Merge MarESA sensitivity assessments with all data within the bsh DF on JNCC code
maresa_bsh_merge = pd.merge(bsh, maresa, left_on='JNCC code', right_on='JNCC_Code', how='outer', indicator=True)

# Create a subset of the maresa_bsh_merge which only contains the bsh without MarESA assessments
bsh_only = maresa_bsh_merge.loc[maresa_bsh_merge['_merge'].isin(['left_only'])]

# Create a subset of the maresa_bsh_merge which only contains the bsh with MarESA assessments
bsh_maresa = maresa_bsh_merge.loc[maresa_bsh_merge['_merge'].isin(['both'])]

#############################################################

# Step 1:
# Assign a full set of pressures with the value 'Unknown' to all bsh data which do not have MarESA Assessments

# Load the full MarESA extract to get a full set of assessed pressures
MarESA = pd.read_excel(r"\\jncc-corpfile\gis\Reference\Marine\Sensitivity\MarESA-Data-Extract-2020-04-24.xlsx",
                       'Biotopes2020-04-24', dtype={'EUNIS_Code': str})

# Identify all pressures and assign as a list
maresa_pressures = list(MarESA['Pressure'].unique())

# Create subset of all unique pressures and NE codes to be used for the append. Achieve this by filtering the MarESA
# DataFrame to only include the unique pressures from the pressures list.
PressuresCodes = MarESA.drop_duplicates(subset=['NE_Code', 'Pressure'], inplace=False)

# Set all values within the 'Resistance', 'ResistanceQoE', 'ResistanceAoE', 'ResistanceDoE', 'Resilience',
# 'ResilienceQoE', 'ResilienceAoE',  'resilienceDoE',  'Sensitivity',  'SensitivityQoE',  'SensitivityAoE',
# 'SensitivityDoE' columns to 'Unknown'

PressuresCodes.loc[:, 'Resistance'] = 'Unknown'
PressuresCodes.loc[:, 'ResistanceQoE'] = 'Unknown'
PressuresCodes.loc[:, 'ResistanceAoE'] = 'Unknown'
PressuresCodes.loc[:, 'ResistanceDoE'] = 'Unknown'
PressuresCodes.loc[:, 'Resilience'] = 'Unknown'
PressuresCodes.loc[:, 'ResilienceQoE'] = 'Unknown'
PressuresCodes.loc[:, 'ResilienceAoE'] = 'Unknown'
PressuresCodes.loc[:, 'resilienceDoE'] = 'Unknown'
PressuresCodes.loc[:, 'Sensitivity'] = 'Unknown'
PressuresCodes.loc[:, 'SensitivityQoE'] = 'Unknown'
PressuresCodes.loc[:, 'SensitivityAoE'] = 'Unknown'
PressuresCodes.loc[:, 'SensitivityDoE'] = 'Unknown'

# Change the following values to 'Not a Number' / nan values to be filled by the fill_metadata() function
PressuresCodes.loc[:, 'EUNIS_Code'] = np.nan
PressuresCodes.loc[:, 'Name'] = np.nan
PressuresCodes.loc[:, 'JNCC_Name'] = np.nan
PressuresCodes.loc[:, 'JNCC_Code'] = np.nan
PressuresCodes.loc[:, 'EUNIS level'] = np.nan

# Create template DF
Template_DF = PressuresCodes

# Create function to complete cross join / create cartesian product between two target DF

def df_crossjoin(df1, df2):
    """
    Make a cross join (cartesian product) between two dataframes by using a constant temporary key.
    Also sets a MultiIndex which is the cartesian product of the indices of the input dataframes.
    :param df1 dataframe 1
    :param df1 dataframe 2

    :return cross join of df1 and df2
    """
    df1.loc[:, '_tmpkey'] = 1
    df2.loc[:, '_tmpkey'] = 1

    res = pd.merge(df1, df2, on='_tmpkey').drop('_tmpkey', axis=1)
    res.index = pd.MultiIndex.from_product((df1.index, df2.index))

    df1.drop('_tmpkey', axis=1, inplace=True)
    df2.drop('_tmpkey', axis=1, inplace=True)

    return res


# Perform cross join to blanket all pressures with unknown values to all EUNIS codes within the correlation_snippet
bsh_unknown_template_cjoin = df_crossjoin(bsh_only, Template_DF)

# Rename columns to match MarESA data
bsh_unknown_template_cjoin.rename(
    columns={
        'Pressure_y': 'Pressure', 'Resistance_y': 'Resistance',
        'Resilience_y': 'Resilience', 'Sensitivity_y': 'Sensitivity', 'JNCC_Code_y': 'JNCC_Code'}, inplace=True)

# Restructure the crossjoined DF to only retain columns of interest
bsh_unknown = bsh_unknown_template_cjoin[
    ['BSH', 'JNCC code', 'JNCC name', 'Pressure', 'Resistance', 'Resilience', 'Sensitivity']
]

# Refine the bsh_maresa dF to match the columns of the newly created bsh_unknown template
bsh_maresa = bsh_maresa[
    ['BSH', 'JNCC code', 'JNCC name', 'Pressure', 'Resistance', 'Resilience', 'Sensitivity']
]

# Append the bsh_unknown into the refined bsh_maresa DF
bsh_maresa_unknowns = bsh_maresa.append(bsh_unknown, ignore_index=True)

#############################################################

# Step 2:
# Prepare the data for aggregation analyses

# Drop the merge column from the DF as this is no longer needed
# bsh_maresa_unknowns = bsh_maresa_unknowns.drop(['_merge'], axis=1, inplace=False)

# Reformat contents of assessment columns within DF
bsh_maresa_unknowns['Sensitivity'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
bsh_maresa_unknowns['Sensitivity'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
bsh_maresa_unknowns['Sensitivity'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
bsh_maresa_unknowns['Sensitivity'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

bsh_maresa_unknowns['Resistance'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
bsh_maresa_unknowns['Resistance'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
bsh_maresa_unknowns['Resistance'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
bsh_maresa_unknowns['Resistance'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

bsh_maresa_unknowns['Resilience'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
bsh_maresa_unknowns['Resilience'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
bsh_maresa_unknowns['Resilience'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
bsh_maresa_unknowns['Resilience'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

# Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
bsh_maresa_unknowns['Sensitivity'].fillna(value='', inplace=True)

#############################################################

# Step 3:
# Aggregation analyses

# Aggregate data together by unique instances of HOCI and Bioregion
bsh_agg = bsh_maresa_unknowns.groupby(['Pressure', 'BSH'])['Sensitivity'].apply(lambda x: ', '.join(x))

# Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
bsh_agg = pd.DataFrame(bsh_agg)

# Reset index of newly created DataFrame to pull out data into 4 individual columns
bsh_agg = bsh_agg.reset_index(inplace=False)

# Define function to count the number of assessment values

# Define all functions which are required within the script to execute aggregation process
# Function Title: counter
def counter(value):
    """Count the total no. of occurrences of each sensitivity (high, medium, low, not sensitive, not relevant,
      no evidence, not assessed, unknown
      Return values to be assigned to new columns through lambda function"""
    counthigh = value.count('High')
    countmedium = value.count('Medium')
    countlow = value.count('Low')
    countns = value.count('Not sensitive')
    countnr = value.count('Not relevant')
    countne = value.count('No evidence')
    countna = value.count('Not assessed')
    countuk = value.count('Unknown')
    return counthigh, countmedium, countlow, countns, countnr, countne, countna, countuk


# Apply the counter() function to the DF to count the occurrence of all assessment values
bsh_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown']] = bsh_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

# Duplicate all count values and assign to new columns to be replaced by string values later in code
bsh_agg['Count_High'] = bsh_agg['High']
bsh_agg['Count_Medium'] = bsh_agg['Medium']
bsh_agg['Count_Low'] = bsh_agg['Low']
bsh_agg['Count_NotSensitive'] = bsh_agg['Not sensitive']
bsh_agg['Count_NotRel'] = bsh_agg['Not relevant']
bsh_agg['Count_NoEvidence'] = bsh_agg['No evidence']
bsh_agg['Count_NotAssessed'] = bsh_agg['Not assessed']
bsh_agg['Count_Unknown'] = bsh_agg['Unknown']

# Create colNames list for use with replacer() function
colNames = ['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed', 'Unknown']

# Define replacer() function to fill a numerical with the repstring being analysed
# Function Title: replacer
def replacer(value, repstring):
    """Perform string replace on each sensitivity count column (one set of duplicates only)"""
    if value == 0:
        return 'NA'
    elif value != 0:
        return repstring

# Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
# assessment score
for eachCol in colNames:
    bsh_agg[eachCol] = bsh_agg[eachCol].apply(lambda x: replacer(x, eachCol))

# Function Title: final_sensitivity
def final_sensitivity(df):
    """Create a return of a string value which gives final sensitivity score dependent on conditional statements"""
    # Create object oriented variable for each column of data from DataFrame (assessed only)
    high = df['High']
    med = df['Medium']
    low = df['Low']
    nsens = df['Not sensitive']
    # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
    nrel = df['Not relevant']
    nev = df['No evidence']
    n_ass = df['Not assessed']
    un = df['Unknown']

    # Create empty list for all string values to be appended into - this will be assigned to each field when data
    # are iterated through using the lambdas function which follows immediately after this function
    value = []
    # Create series of conditional statements to append string values into the empty list ('value') if conditional
    # statements are fulfilled
    if 'High' in high:
        h = 'High'
        value.append(h)
    if 'Medium' in med:
        m = 'Medium'
        value.append(m)
    if 'Low' in low:
        lo = 'Low'
        value.append(lo)
    if 'Not sensitive' in nsens:
        ns = 'Not sensitive'
        value.append(ns)
    if 'High' not in high and 'Medium' not in med and 'Low' not in low and 'Not sensitive' not in nsens:
        if 'Not relevant' in nrel:
            nr = 'Not relevant'
            value.append(nr)
        if 'No evidence' in nev:
            ne = 'No evidence'
            value.append(ne)
        if 'Not assessed' in n_ass:
            nass = 'Not assessed'
            value.append(nass)
    if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens and 'NA' in nrel and 'NA' in nev and \
            'NA' in n_ass:
        un = 'Unknown'
        value.append(un)

    s = ', '.join(set(value))
    return str(s)


# Use lambda function to apply final_sensitivity() function to each row within the DataFrame
bsh_agg['AggregatedSensitivity'] = bsh_agg.apply(lambda df: final_sensitivity(df), axis=1)


# Define function to calculate the total count of all assessed values
# Function Title: combine_assessedcounts
def combine_assessedcounts(df):
    """Conditional statements which combine assessed count data and return as string value"""
    # Create object oriented variable for each column of data from DataFrame (assessed only)
    high = df['High']
    med = df['Medium']
    low = df['Low']
    nsens = df['Not sensitive']
    # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
    nrel = df['Not relevant']
    nev = df['No evidence']
    n_ass = df['Not assessed']
    un = df['Unknown']

    # Create empty list for all string values to be appended into - this will be assigned to each field when data
    # are iterated through using the lambdas function which follows immediately after this function
    value = []
    # Create series of conditional statements to append string values into the empty list ('value') if conditional
    # statements are fulfilled
    if 'High' in high:
        h = 'H(' + str(df['Count_High']) + ')'
        value.append(h)
    if 'Medium' in med:
        m = 'M(' + str((df['Count_Medium'])) + ')'
        value.append(m)
    if 'Low' in low:
        lo = 'L(' + str(df['Count_Low']) + ')'
        value.append(lo)
    if 'Not sensitive' in nsens:
        ns = 'NS(' + str(df['Count_NotSensitive']) + ')'
        value.append(ns)
    if 'Not relevant' in nrel:
        nr = 'NR(' + str(df['Count_NotRel']) + ')'
        value.append(nr)
    if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens and 'NA' in nrel:
        # if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens:
        # if 'Not relevant' in nrel:
        #     nr = 'Not Applicable'
        #     value.append(nr)
        if 'No evidence' in nev:
            ne = 'Not Applicable'
            value.append(ne)
        if 'Not assessed' in n_ass:
            nass = 'Not Applicable'
            value.append(nass)
        if 'Unknown' in un:
            unk = 'Not Applicable'
            value.append(unk)
    s = ', '.join(set(value))
    return str(s)


# Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
bsh_agg['AssessedCount'] = bsh_agg.apply(lambda df: combine_assessedcounts(df), axis=1)


# Define function to calculate the total count of all unassessed values
# Function Title: combine_unassessedcounts
def combine_unassessedcounts(df):
    """Conditional statements which combine unassessed count data and return as string value"""
    # Create object oriented variable for each column of data from DataFrame (assessed only)
    # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
    nrel = df['Not relevant']
    nev = df['No evidence']
    n_ass = df['Not assessed']
    un = df['Unknown']

    # Create empty list for all string values to be appended into - this will be assigned to each field when data
    # are iterated through using the lambdas function which follows immediately after this function

    values = []

    # Create series of conditional statements to append string values into the empty list ('value') if conditional
    # statements are fulfilled

    # if 'Not relevant' in nrel:
    #     nr = 'NR(' + str(df['Count_NotRel']) + ')'
    #     values.append(nr)
    if 'No evidence' in nev:
        ne = 'NE(' + str(df['Count_NoEvidence']) + ')'
        values.append(ne)
    if 'Not assessed' in n_ass:
        na = 'NA(' + str(df['Count_NotAssessed']) + ')'
        values.append(na)
    if 'Unknown' in un:
        unk = 'UN(' + str(df['Count_Unknown']) + ')'
        values.append(unk)
    # if 'NA' in nrel and 'NA' in nev and 'NA' in n_ass and 'NA' in un:
    if 'NA' in nev and 'NA' in n_ass and 'NA' in un:
        napp = 'Not Applicable'
        values.append(napp)
    s = ', '.join(set(values))
    return str(s)


# Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
bsh_agg['UnassessedCount'] = bsh_agg.apply(lambda df: combine_unassessedcounts(df), axis=1)


# Define function to create confidence value for the aggregation process
# Function Title: create_confidence
def create_confidence(df):
    """Divide the total assessed counts by the total count of all data and return as numerical value"""
    # Pull in assessed values counts
    count_high = df['Count_High']
    count_med = df['Count_Medium']
    count_low = df['Count_Low']
    count_ns = df['Count_NotSensitive']

    # Pull in unassessed values counts
    count_nr = df['Count_NotRel']
    count_ne = df['Count_NoEvidence']
    count_na = df['Count_NotAssessed']
    count_unk = df['Count_Unknown']

    # Create ratio calculation
    total_ass = count_high + count_med + count_low + count_ns
    total = total_ass + count_ne + count_na + count_unk

    return round(total_ass / total, 3) if total else 0


# Use lambda function to apply create_confidence() function to the DataFrame
bsh_agg['AggregationConfidenceValue'] = bsh_agg.apply(lambda df: create_confidence(df), axis=1)

# Function Title: categorise_confidence
def categorise_confidence(df, column):
    """Partition and categorise confidence values by quantile intervals"""
    if column == 'AggregationConfidenceValue':
        value = df[column]
        if value < 0.33:
            return 'Low'
        elif value >= 0.33 and value < 0.66:
            return ' Medium'
        elif value >= 0.66:
            return 'High'


# Create categories for confidence values: EUNIS Level 2
bsh_agg['AggregationConfidenceScore'] = bsh_agg.apply(
    lambda df: categorise_confidence(df, 'AggregationConfidenceValue'), axis=1)

#############################################################

# Step 4:
# Post-analyses formatting

# Refine DF to retain columns of interest
bsh_agg = bsh_agg[
['Pressure', 'BSH','AggregatedSensitivity', 'AssessedCount', 'UnassessedCount', 'AggregationConfidenceValue',
 'AggregationConfidenceScore']
]

# Export DF for use

# Define folder file path to be saved into
outpath = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\DeepSeaBSHAggregation"
# Define file name to save, categorised by date
filename = "DeepSeaBSHSensitivityAggregation_" + (time.strftime("%Y%m%d") + ".csv")
# Run the output DF.to_csv method
bsh_agg.to_csv(outpath + "\\" + filename, sep=',')