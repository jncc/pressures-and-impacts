########################################################################################################################

# Title: Sensitivity Aggregation

# Authors: Matear, L.(2018)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:    Read in all data from Bioregions extract and MarLIN-MarESA Biotopes extract sensitivity data
#                        to be aggregated between EUNIS levels. This task allows the user to explore the connectivity
#                        of data across varying resolutions of detail.
#
#                        To ensure no permanent alterations are made to the master documents, all data used within this
#                        script are copies of the original files.
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk

########################################################################################################################

# Section 1: Introduction

# This script allows the user to aggregate resistance data across varying tiers of EUNIS hierarchies. The main purpose
# of this code is to develop a mechanism through which the user can identify which completed assessments sit within
# which tiers of the EUNIS hierarchy. The outputs of these analyses should enable the user to map the spatial
# distribution of assessments made at EUNIS Levels 5 and 6 at less detailed resolutions (e.g. EUNIS Level 2 and / or
# EUNIS Level 3).

# Aggregation Process Outline

# See 'Methodology Infographic' file within the 'Sensitivity Assessments' folder


########################################################################################################################

# Section 2: Initial setup and data import

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required
import os
import pandas as pd

# Set working directory for file access
os.chdir(r'ENTER FILE PATH HERE')

# Load required data and assign to object oriented variables
bioregions = pd.read_excel('ENTER FILE NAME HERE', 'ENTER TARGET TAB HERE')
maresa = pd.read_excel('ENTER FILE NAME HERE', 'ENTER TARGET TAB HERE')


########################################################################################################################

# Section 2a: Defining functions (data formatting)

# Define all functions which are required within the script to format data for aggregation process
# Function Title: df_clean
def df_clean(df):
    """User defined function to refine dataset - remove whitespace
     and Inshore / No Biotope Presence data"""
    # Toggle this on / off to get offshore or all data together
    df.drop(df[df.BiotopePresence == 'Inshore only'].index, inplace=True)
    # Refine dataset to only include data for which BiotopePresence == 'Poss' or 'Yes'
    df.drop(df[df.BiotopePresence == 'No'].index, inplace=True)
    df['EUNIS_Code'].str.strip()
    df['Resistance'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    df['Resistance'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    df['Resistance'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    return df


# Function Title: unwanted_char
def unwanted_char(df, column):
    """User defined function to refine dataset - remove unwanted special characters and acronyms from resistance
    scores. User must pass the DataFrame and the column (as a string) as the arguments to the parentheses of the
    function"""
    for eachField in df[column]:
        eachField.replace(r"\\s*zz(][^\\]+\\)", "")
    return df


# Function Title: eunis_col
def eunis_col(row):
    """User defined function to pull out all entries in EUNIS_Code column and create returns based on string
    slices of the EUNIS data. This must be used with df.apply() and a lambda function.

    e.g. fact_tbl[['Level_1', 'Level_2', 'Level_3',
          'Level_4', 'Level_5', 'Level_6']] = fact_tbl.apply(lambda row: pd.Series(eunis_col(row)), axis=1)"""

    # Create object oriented variable to store EUNIS_Code data
    ecode = row['EUNIS_Code']
    # Create if / elif conditions to produce response dependent on the string length of the inputted data.
    if len(ecode) == 1:
        return ecode[0:1], None, None, None, None, None
    elif len(ecode) == 2:
        return ecode[0:1], ecode[0:2], None, None, None, None
    elif len(ecode) == 4:
        return ecode[0:1], ecode[0:2], ecode[0:4], None, None, None
    elif len(ecode) == 5:
        return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], None, None
    elif len(ecode) == 6:
        return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], None
    elif len(ecode) == 7:
        return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], ecode[0:7]


# Function Title: eunis_lvl
def eunis_lvl(row):
    """User defined function to pull out all data from the column 'EUNIS_Code' and return an integer dependant on the
    EUNIS level in response"""

    # Create object oriented variable to store EUNIS_Code data
    ecode = row['EUNIS_Code']
    # Create if / elif conditions to produce response dependent on the string length of the inputted data
    if len(ecode) == 1:
        return '1'
    elif len(ecode) == 2:
        return '2'
    elif len(ecode) == 4:
        return '3'
    elif len(ecode) == 5:
        return '4'
    elif len(ecode) == 6:
        return '5'
    elif len(ecode) == 7:
        return '6'


########################################################################################################################

# Section 2b: Defining functions (aggregation process)

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


# Function Title: replacer
def replacer(value, repstring):
    """Perform string replace on each sensitivity count column (one set of duplicates only)"""
    if value == 0:
        return 'NA'
    elif value != 0:
        return repstring


# Function Title: create_resistance
def create_resistance(df):
    """Series of conditional statements which return a string value of all assessment values
    contained within individual columns"""
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

    # Create empty list for all string values to be appended into - this will be assigned to each field when data are
    # iterated through using the lambdas function which follows immediately after this function
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
    if 'Not relevant' in nrel:
        nr = 'Not relevant'
        value.append(nr)
    if 'No evidence' in nev:
        ne = 'No evidence'
        value.append(ne)
    if 'Not assessed' in n_ass:
        nass = 'Not assessed'
        value.append(nass)
    if 'Unknown' in un:
        unk = 'Unknown'
        value.append(unk)
    s = ', '.join(value)
    return str(s)


# Function Title: final_Resistance
def final_resistance(df):
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

    # Create empty list for all string values to be appended into - this will be assigned to each field when data are
    # iterated through using the lambdas function which follows immediately after this function
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
    s = ', '.join(value)
    return str(s)


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

    # Create empty list for all string values to be appended into - this will be assigned to each field when data are
    # iterated through using the lambdas function which follows immediately after this function
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
    s = ', '.join(value)
    return str(s)


# Function Title: combine_unassessedcounts
def combine_unassessedcounts(df):
    """Conditional statements which combine unassessed count data and return as string value"""
    # Create object oriented variable for each column of data from DataFrame (assessed only)
    # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
    nrel = df['Not relevant']
    nev = df['No evidence']
    n_ass = df['Not assessed']
    un = df['Unknown']

    # Create empty list for all string values to be appended into - this will be assigned to each field when data are
    # iterated through using the lambdas function which follows immediately after this function

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
    s = ', '.join(values)
    return str(s)


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

    # Create ratio calculation - count_nr added to total ass and removed from total
    # total_ass = count_high + count_med + count_low + count_ns + count_nr

    # Create new test version without NR
    total_ass = count_high + count_med + count_low + count_ns
    total = total_ass + count_ne + count_na + count_unk

    return total_ass / total if total else 0


# Function Title: categorise_confidence
def categorise_confidence(df, column):
    """Partition and categorise confidence values by quantile intervals"""
    if column == 'L2_AssessmentConfidence':
        value = df[column]
        if value < 0.33:
            return 'Low'
        elif value >= 0.33 and value < 0.66:
            return ' Medium'
        elif value >= 0.66:
            return 'High'
    elif column == 'L3_AssessmentConfidence':
        value = df[column]
        if value < 0.33:
            return 'Low'
        elif value >= 0.33 and value < 0.66:
            return ' Medium'
        elif value >= 0.66:
            return 'High'
    elif column == 'L4_AssessmentConfidence':
        value = df[column]
        if value < 0.33:
            return 'Low'
        elif value >= 0.33 and value < 0.66:
            return ' Medium'
        elif value == 0.66:
            return 'High'
    elif column == 'L5_AssessmentConfidence':
        return 'NA'


# Function Title: column4
def column4(df):
    """Sample Level_5 column and return string variables sliced within the range [0:5]"""
    value = df['Level_5']
    sample = value[0:5]
    return sample


# Function Title: column3
def column3(df, column):
    """User defined function to sample Level_4 column and return string variables sliced within the range [0:4]"""
    value = df[column]
    sample = value[0:4]
    return sample


# Function Title: column2
def column2(df, column):
    """User defined function to sample Level_4 column and return string variables sliced within the range [0:4]"""
    value = df[column]
    sample = value[0:2]
    return sample


########################################################################################################################

# Section 2c: Data formatting

# Rename bioregions column to facilitate merge
bioregions.rename(columns={'HabitatCode': 'EUNIS_Code'}, inplace=True)

# Merge data to create fact_tbl dataset
fact_tbl = pd.merge(bioregions, maresa, on='EUNIS_Code')

# Remove unwanted data by passing the fact_tbl DF to the df_clean() function
df_clean(fact_tbl)

# Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
fact_tbl['Resistance'].fillna(value='', inplace=True)

# Remove unwanted special characters from data by passing the fact_tbl to the unwanted_char() function
fact_tbl = unwanted_char(fact_tbl, 'Resistance')

# Create individual EUNIS level columns in fact_tbl using a lambda function and apply() method on 'EUNIS_Code' column on
# DF.
fact_tbl[['Level_1', 'Level_2', 'Level_3',
          'Level_4', 'Level_5', 'Level_6']] = fact_tbl.apply(lambda row: pd.Series(eunis_col(row)), axis=1)

# Create new 'EUNIS_Level' column which indicates the numerical value of the EUNIS level by passing the fact_tbl to the
# eunis_lvl() function.
fact_tbl['EUNIS_Level'] = fact_tbl.apply(lambda row: eunis_lvl(row), axis=1)


########################################################################################################################

# Section 3: Level 6 to 5 subsetting data

# The following section of code performs the aggregation of data from EUNIS Level 6 to EUNIS Level 5. All data which
# have undergone an aggregation comprise biotopes which have assessments at Level 6, but not at Level 5. Therefore,
# this will allow the end user to identify which sensitivity scores are relevant to lower EUNIS levels when an
# assessment has not been completed.

# Extract all level 6 data only and assign to object oriented variable to be aggregated to level 5
L6 = pd.DataFrame(fact_tbl.loc[fact_tbl['EUNIS_Level'].isin(['6'])])

# Extract all original level 5 data and assign to object oriented variable
L5_orig = pd.DataFrame(fact_tbl.loc[fact_tbl['EUNIS_Level'].isin(['5'])])

# Assign data differences to new object oriented variable using outer merge between data frames
L56_merge = pd.merge(L6, L5_orig, how='outer', on=['Level_5', 'Pressure'], indicator=True)

# Filter merged data by left only data to subset EUNIS level 6 data which does not have a level 5 assessment
L56_diff = pd.DataFrame(L56_merge[L56_merge['_merge'] == 'left_only'])

#       Clean outer merge of excess data from right only channel
L56_diff = L56_diff.drop([
    'ID_x_y', 'SubregionName_y', 'RegionName_y', 'BiotopePresence_y', 'EUNIS_Code_y',
    'HabitatName_y', 'Gaps_y', 'ID_y_y', 'Name_y', 'NE_Code_y', 'Resistance_y', 'ResistanceQoE_y',
    'ResistanceAoE_y', 'ResistanceDoE_y', 'Resilience_y', 'ResilienceQoE_y', 'ResilienceAoE_y',
    'resilienceDoE_y', 'Sensitivity_y', 'SensitivityQoE_y', 'SensitivityAoE_y', 'SensitivityDoE_y',
    'url_y', 'Level_1_y', 'Level_2_y', 'Level_3_y', 'Level_4_y', 'Level_6_y', 'EUNIS_Level_y',
    '_merge'], axis=1, inplace=False)


########################################################################################################################

# Section 4a: Level 6 to 5 aggregation (formatting)

# The following body of code begins the initial steps of the aggregation process from level 6 to level 5

# Group data by Level_5, Pressure, SubregionName and apply resistance values to list using lambdas function and
# .apply() method
L5_agg = L56_diff.groupby(['Level_5', 'Pressure', 'SubregionName_x']
                          )['Resistance_x'].apply(lambda x: ', '.join(x))

# Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
L5_agg = pd.DataFrame(L5_agg)

# Reset index of newly created DataFrame to pull out data into 4 individual columns
L5_agg = L5_agg.reset_index(inplace=False)

# Reset columns within L5_agg DF
L5_agg.columns = ['Level_5', 'Pressure', 'SubregionName', 'Resistance']

# Apply the counter() function to the DF to count the occurrence of all assessment values
L5_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown']] = L5_agg.apply(lambda df: pd.Series(counter(df['Resistance'])), axis=1)

# Duplicate all count values and assign to new columns to be replaced by string values later in code
L5_agg['Count_High'] = L5_agg['High']
L5_agg['Count_Medium'] = L5_agg['Medium']
L5_agg['Count_Low'] = L5_agg['Low']
L5_agg['Count_NotSensitive'] = L5_agg['Not sensitive']
L5_agg['Count_NotRel'] = L5_agg['Not relevant']
L5_agg['Count_NoEvidence'] = L5_agg['No evidence']
L5_agg['Count_NotAssessed'] = L5_agg['Not assessed']
L5_agg['Count_Unknown'] = L5_agg['Unknown']

# Reassign L5_agg DataFrame to L5_sens for sensitivity aggregation
L5_res = L5_agg

# Create colNames list for use with replacer() function
colNames = ['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed', 'Unknown']

# Run replacer() function on one set of newly duplicated columns to convert integers to string values of the assessment
# score
for eachCol in colNames:
    L5_res[eachCol] = L5_res[eachCol].apply(lambda x: replacer(x, eachCol))


########################################################################################################################

# Section 4b: Level 6 to 5 aggregation (aggregation)

# Use lambda function to apply create_resistance() function to each row within the DataFrame
L5_res['L5_Resistance'] = L5_res.apply(lambda df: create_resistance(df), axis=1)

# Use lambda function to apply final_resistance() function to each row within the DataFrame
L5_res['L5_FinalResistance'] = L5_res.apply(lambda df: final_resistance(df), axis=1)

# Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
L5_res['L5_AssessedCount'] = L5_res.apply(lambda df: combine_assessedcounts(df), axis=1)

# Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
L5_res['L5_UnassessedCount'] = L5_res.apply(lambda df: combine_unassessedcounts(df), axis=1)

# Apply column4() function to L5_res DataFrame to create new Level_4 column
L5_res['Level_4'] = L5_res.apply(lambda df: pd.Series(column4(df)), axis=1)

# Use lambda function to apply create_confidence() function to the DataFrame
L5_res['L5_AssessmentConfidence'] = L5_res.apply(lambda df: create_confidence(df), axis=1)


########################################################################################################################

# Section 4c: Level 6 to 5 aggregation (combine aggregated and existing assessments)

# Create object oriented variables to be used within section 4c (original L5 data and L5 aggregated data)
L5_agg_res = L5_res
L5_orig_sub = L5_orig

#       Drop unwanted columns to allow for both DataFrames to have matching indices
L5_agg_res = L5_agg_res.drop(['Resistance', 'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
                              'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
                              'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
                              'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

# Rename newly created L5_sensitivity column 'Sensitivity' to match both DataFrames
L5_agg_res.rename(columns={'L5_Resistance': 'Resistance'}, inplace=True)

# Drop unwanted columns from 'L5_orig_sub' DataFrame
L5_orig_sub = L5_orig_sub.drop(['ID_x', 'RegionName', 'BiotopePresence', 'EUNIS_Code', 'HabitatName', 'Gaps', 'ID_y',
                                'Name',
                                'NE_Code', 'Resilience', 'ResistanceQoE', 'ResistanceAoE', 'ResistanceDoE',
                                'Sensitivity', 'ResilienceQoE', 'ResilienceAoE', 'resilienceDoE', 'SensitivityQoE',
                                'SensitivityAoE', 'SensitivityDoE', 'url', 'Level_1', 'Level_2', 'Level_3', 'Level_4',
                                'Level_6', 'EUNIS_Level', 'JNCC_Code'], axis=1, inplace=False)


# Apply the counter() function to the L5_orig_sub DataFrame to count the occurrence of all assessment values
L5_orig_sub[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
             'Unknown']] = L5_orig_sub.apply(lambda df: pd.Series(counter(df['Resistance'])), axis=1)

# Duplicate all L5_orig_sub count values and assign to new columns to be replaced by string values later in code
L5_orig_sub['Count_High'] = L5_orig_sub['High']
L5_orig_sub['Count_Medium'] = L5_orig_sub['Medium']
L5_orig_sub['Count_Low'] = L5_orig_sub['Low']
L5_orig_sub['Count_NotSensitive'] = L5_orig_sub['Not sensitive']
L5_orig_sub['Count_NotRel'] = L5_orig_sub['Not relevant']
L5_orig_sub['Count_NoEvidence'] = L5_orig_sub['No evidence']
L5_orig_sub['Count_NotAssessed'] = L5_orig_sub['Not assessed']
L5_orig_sub['Count_Unknown'] = L5_orig_sub['Unknown']

# Run replacer() function on one set of newly duplicated columns to convert integers to string values of the assessment
# score
for eachCol in colNames:
    L5_orig_sub[eachCol] = L5_orig_sub[eachCol].apply(lambda x: replacer(x, eachCol))

# Use lambda function to apply final_resistance() function to each row within the DataFrame
L5_orig_sub['L5_FinalResistance'] = L5_orig_sub.apply(lambda df: final_resistance(df), axis=1)

# Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
L5_orig_sub['L5_AssessedCount'] = L5_orig_sub.apply(lambda df: combine_assessedcounts(df), axis=1)

# Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
L5_orig_sub['L5_UnassessedCount'] = L5_orig_sub.apply(lambda df: combine_unassessedcounts(df), axis=1)

# Apply column4() function to L5_orig_sub DataFrame to create new Level_4 column
L5_orig_sub['Level_4'] = L5_orig_sub.apply(lambda df: pd.Series(column4(df)), axis=1)

# Use lambda function to apply create_confidence() function to the DataFrame
L5_orig_sub['L5_AssessmentConfidence'] = L5_orig_sub.apply(lambda df: create_confidence(df), axis=1)

# Drop all unwanted columns from L5_orig_sub DataFrame
L5_orig_sub.drop(['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence',
                  'Not assessed', 'Unknown', 'Count_High', 'Count_Medium', 'Count_Low', 'Count_NotSensitive',
                  'Count_NotRel', 'Count_NoEvidence', 'Count_NotAssessed', 'Count_Unknown',
                  'JNCC_Name'], axis=1, inplace=True)

# Append L5_orig_sub DataFrame with newly developed L5_agg_sens DataFrame
L5_all = L5_orig_sub.append(L5_agg_res, sort=True)

# Drop 'Classification level' column as this is not currently used in the aggregation process.
# L5_all.drop(['Classification_Level'], axis=1, inplace=True)

# Format columns into correct order
L5_all = L5_all[['Pressure', 'SubregionName', 'Level_4', 'Level_5', 'Resistance', 'L5_FinalResistance',
                 'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AssessmentConfidence']]


########################################################################################################################

# Section 4d: Level 6 to 5 aggregation (creating an aggregated export)

# Create DataFrame for master DataFrame at end of script
L5_export = L5_all

# Drop unwanted columns from L5_export DataFrame
L5_export = L5_export.drop(['Resistance'], axis=1, inplace=False)

# Rename and reorder columns within L5_export
L5_export = L5_export[['Level_4', 'Pressure', 'SubregionName', 'Level_5', 'L5_FinalResistance',
                       'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AssessmentConfidence']]


########################################################################################################################

# Section 5a: Level 5 to 4 aggregation (formatting)

# The following body of code begins the initial steps of the aggregation process from level 5 to level 4

# Group data by Level_4, Pressure, SubregionName and apply resistance values to list using lambdas function and .apply()
# method
L4_agg = L5_all.groupby(['Level_4', 'Pressure', 'SubregionName'
                         ])['Resistance'].apply(lambda x: ', '.join(x))

# Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
L4_agg = pd.DataFrame(L4_agg)

# Reset index of newly created DataFrame to pull out data into 4 individual columns
L4_agg = L4_agg.reset_index(inplace=False)

# Reset columns within L4_agg DataFrame
L4_agg.columns = ['Level_4', 'Pressure', 'SubregionName', 'Resistance']

# Apply the counter() function to the DataFrame to count the occurrence of all assessment values
L4_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown']] = L4_agg.apply(lambda df: pd.Series(counter(df['Resistance'])), axis=1)

# Duplicate all count values and assign to new columns to be replaced by string values later
L4_agg['Count_High'] = L4_agg['High']
L4_agg['Count_Medium'] = L4_agg['Medium']
L4_agg['Count_Low'] = L4_agg['Low']
L4_agg['Count_NotSensitive'] = L4_agg['Not sensitive']
L4_agg['Count_NotRel'] = L4_agg['Not relevant']
L4_agg['Count_NoEvidence'] = L4_agg['No evidence']
L4_agg['Count_NotAssessed'] = L4_agg['Not assessed']
L4_agg['Count_Unknown'] = L4_agg['Unknown']

# Reassign L4_agg DataFrame to L4_sens for sensitivity aggregation
L4_res = L4_agg

# Run replacer() function on one set of newly duplicated columns to convert integers to string values of the assessment
# score
for eachCol in colNames:
    L4_res[eachCol] = L4_res[eachCol].apply(lambda x: replacer(x, eachCol))


########################################################################################################################

# Section 5b: Level 5 to 4 aggregation (aggregation)

# Use lambda function to apply create_resistance() function to each row within the DataFrame
L4_res['L4_Resistance'] = L4_res.apply(lambda df: create_resistance(df), axis=1)

# Use lambda function to apply final_resistance() function to each row within the DataFrame
L4_res['L4_FinalResistance'] = L4_res.apply(lambda df: final_resistance(df), axis=1)

# Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
L4_res['L4_AssessedCount'] = L4_res.apply(lambda df: combine_assessedcounts(df), axis=1)

# Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
L4_res['L4_UnassessedCount'] = L4_res.apply(lambda df: combine_unassessedcounts(df), axis=1)

# Apply column3() function to L4_sens DataFrame to create new Level_3 column
L4_res['Level_3'] = L4_res.apply(lambda df: pd.Series(column3(df, 'Level_4')), axis=1)

# Use lambda function to apply create_confidence() function to the DataFrame
L4_res['L4_AssessmentConfidence'] = L4_res.apply(lambda df: create_confidence(df), axis=1)

# Format columns into correct order
L4_res = L4_res[['Level_3', 'Pressure', 'SubregionName', 'Level_4', 'Resistance', 'L4_Resistance',
                 'L4_FinalResistance', 'L4_AssessedCount', 'L4_UnassessedCount', 'L4_AssessmentConfidence']]


########################################################################################################################

# Section 5c: Level 5 to 4 aggregation (creating an aggregated export)

# Create DataFrame for master DataFrame at end of script
L4_export = L4_res

# Drop unwanted columns from L4_export DataFrame
L4_export = L4_export.drop(['Resistance', 'L4_Resistance'], axis=1, inplace=False)


########################################################################################################################

# Section 6a: Level 4 to 3 aggregation (formatting)

# The following body of code begins the initial steps of the aggregation process from level 4 to level 3

# Group data by Level_3, Pressure, SubregionName and apply resistance values to list using lambdas function and
# .apply() method
L3_agg = L4_res.groupby(['Level_3', 'Pressure', 'SubregionName'
                         ])['Resistance'].apply(lambda x: ', '.join(x))

# Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
L3_agg = pd.DataFrame(L3_agg)

# Reset index of newly created DataFrame to pull out data into 4 individual columns
L3_agg = L3_agg.reset_index(inplace=False)

# Reset columns within L3_agg DataFrame
L3_agg.columns = ['Level_3', 'Pressure', 'SubregionName', 'Resistance']

# Apply the counter() function to the DataFrame to count the occurrence of all assessment values
L3_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown']] = L3_agg.apply(lambda df: pd.Series(counter(df['Resistance'])), axis=1)

# Duplicate all count values and assign to new columns to be replaced by string values later
L3_agg['Count_High'] = L3_agg['High']
L3_agg['Count_Medium'] = L3_agg['Medium']
L3_agg['Count_Low'] = L3_agg['Low']
L3_agg['Count_NotSensitive'] = L3_agg['Not sensitive']
L3_agg['Count_NotRel'] = L3_agg['Not relevant']
L3_agg['Count_NoEvidence'] = L3_agg['No evidence']
L3_agg['Count_NotAssessed'] = L3_agg['Not assessed']
L3_agg['Count_Unknown'] = L3_agg['Unknown']

# Reassign L3_agg DataFrame to L3_sens for sensitivity aggregation
L3_res = L3_agg

# Run replacer() function on one set of newly duplicated columns to convert integers to string values of the assessment
# score
for eachCol in colNames:
    L3_res[eachCol] = L3_res[eachCol].apply(lambda x: replacer(x, eachCol))


########################################################################################################################

# Section 6b: Level 4 to 3 aggregation (aggregation)

# Aggregate data using the functions defined in section 2b

# Use lambda function to apply create_resistance() function to each row within the DataFrame
L3_res['L3_Resistance'] = L3_res.apply(lambda df: create_resistance(df), axis=1)

# Use lambda function to apply final_resistance() function to each row within the DataFrame
L3_res['L3_FinalResistance'] = L3_res.apply(lambda df: final_resistance(df), axis=1)

# Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
L3_res['L3_AssessedCount'] = L3_res.apply(lambda df: combine_assessedcounts(df), axis=1)

# Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
L3_res['L3_UnassessedCount'] = L3_res.apply(lambda df: combine_unassessedcounts(df), axis=1)

# Apply column2() function to L3_res DataFrame to create new Level_2 column
L3_res['Level_2'] = L3_res.apply(lambda df: pd.Series(column2(df, 'Level_3')), axis=1)

# Use lambda function to apply create_confidence() function to the DataFrame
L3_res['L3_AssessmentConfidence'] = L3_res.apply(lambda df: create_confidence(df), axis=1)

# Drop unwanted data from L3_sens DataFrame
L3_res = L3_res.drop(['Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
                      'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
                      'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
                      'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)


########################################################################################################################

# Section 6c: Level 4 to 3 aggregation (creating an aggregated export)

# Create DataFrame for master DataFrame at end of script
L3_export = L3_res

# Drop unwanted columns from L3_export DataFrame
L3_export = L3_export.drop(['L3_Resistance', 'Resistance'], axis=1, inplace=False)


########################################################################################################################

# Section 7a: Level 3 to 2 aggregation (formatting)

# The following body of code begins the initial steps of the aggregation process from level 3 to level 2

# Group data by Level_2, Pressure, SubregionName and apply resistance values to list using lambdas function and
# .apply() method
L2_agg = L3_res.groupby(['Level_2', 'Pressure', 'SubregionName'
                         ])['Resistance'].apply(lambda x: ', '.join(x))

# Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
L2_agg = pd.DataFrame(L2_agg)

# Reset index of newly created DataFrame to pull out data into 4 individual columns
L2_agg = L2_agg.reset_index(inplace=False)

# Reset columns within L2_agg DataFrame
L2_agg.columns = ['Level_2', 'Pressure', 'SubregionName', 'Resistance']

# Apply the counter() function to the DataFrame to count the occurrence of all assessment values
L2_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown']] = L2_agg.apply(lambda df: pd.Series(counter(df['Resistance'])), axis=1)

# Duplicate all count values and assign to new columns to be replaced by string values later
L2_agg['Count_High'] = L2_agg['High']
L2_agg['Count_Medium'] = L2_agg['Medium']
L2_agg['Count_Low'] = L2_agg['Low']
L2_agg['Count_NotSensitive'] = L2_agg['Not sensitive']
L2_agg['Count_NotRel'] = L2_agg['Not relevant']
L2_agg['Count_NoEvidence'] = L2_agg['No evidence']
L2_agg['Count_NotAssessed'] = L2_agg['Not assessed']
L2_agg['Count_Unknown'] = L2_agg['Unknown']

# Reassign L2_agg DataFrame to L2_sens for sensitivity aggregation
L2_res = L2_agg

# Run replacer() function on one set of newly duplicated columns to convert integers to string values of the assessment
# score
for eachCol in colNames:
    L2_res[eachCol] = L2_res[eachCol].apply(lambda x: replacer(x, eachCol))


########################################################################################################################

# Section 7a: Level 3 to 2 aggregation (aggregation)

# Aggregate data using the functions defined in section 2b

# Use lambda function to apply create_resistance() function to each row within the DataFrame
L2_res['L2_Resistance'] = L2_res.apply(lambda df: create_resistance(df), axis=1)

# Use lambda function to apply final_resistance() function to each row within the DataFrame
L2_res['L2_FinalResistance'] = L2_res.apply(lambda df: final_resistance(df), axis=1)

# Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
L2_res['L2_AssessedCount'] = L2_res.apply(lambda df: combine_assessedcounts(df), axis=1)

# Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
L2_res['L2_UnassessedCount'] = L2_res.apply(lambda df: combine_unassessedcounts(df), axis=1)

# Use lambda function to apply create_confidence() function to the DataFrame
L2_res['L2_AssessmentConfidence'] = L2_res.apply(lambda df: create_confidence(df), axis=1)

# Drop unwanted data from L2_sens DataFrame
L2_res = L2_res.drop(['Resistance', 'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
                      'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
                      'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
                      'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

# Format columns into correct order
L2_res = L2_res[['Level_2', 'Pressure', 'SubregionName', 'L2_FinalResistance', 'L2_Resistance',
                 'L2_AssessedCount', 'L2_UnassessedCount', 'L2_AssessmentConfidence']]


########################################################################################################################

# Section 7b: Level 3 to 2 aggregation (creating an aggregated export)

# Create DataFrame for master DataFrame at end of script
L2_export = L2_res

# Drop unwanted columns from L2_export DataFrame
L2_export = L2_export.drop(['L2_Resistance'], axis=1, inplace=False)


########################################################################################################################

# Section 8: Creating a MasterFrame

# Combine all exported DataFrames into one MasterFrame for export of aggregation work

# Merge EUNIS Levels 2 and 3
L2L3 = pd.merge(L2_export, L3_export)

# Merge EUNIS Levels 3 and 4
L3L4 = pd.merge(L2L3, L4_export)

# Merge EUNIS Levels 4 and 5
MasterFrame = pd.merge(L3L4, L5_export)


########################################################################################################################

# Section 9: Categorising confidence values

# Assess all confidence scores using the categorise_confidence() function developed in Section 2b and store information
# in a correlating Confidence Category column.

# Create categories for confidence values: EUNIS Level 5
MasterFrame['L5_ConfidenceCategory'] = MasterFrame.apply(
    lambda df: categorise_confidence(df, 'L5_AssessmentConfidence'), axis=1)

# Create categories for confidence values: EUNIS Level 4
MasterFrame['L4_ConfidenceCategory'] = MasterFrame.apply(
    lambda df: categorise_confidence(df, 'L4_AssessmentConfidence'), axis=1)

# Create categories for confidence values: EUNIS Level 3
MasterFrame['L3_ConfidenceCategory'] = MasterFrame.apply(
    lambda df: categorise_confidence(df, 'L3_AssessmentConfidence'), axis=1)

# Create categories for confidence values: EUNIS Level 2
MasterFrame['L2_ConfidenceCategory'] = MasterFrame.apply(
    lambda df: categorise_confidence(df, 'L2_AssessmentConfidence'), axis=1)


########################################################################################################################

# Section 10: Exporting the MasterFrame

# Create correct order for columns within MasterFrame
MasterFrame = MasterFrame[['Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_AssessedCount',
                           'L2_UnassessedCount', 'L2_AssessmentConfidence', 'L2_ConfidenceCategory', 'Level_3',
                           'L3_FinalResistance', 'L3_AssessedCount', 'L3_UnassessedCount', 'L3_AssessmentConfidence',
                           'L3_ConfidenceCategory', 'Level_4', 'L4_FinalResistance', 'L4_AssessedCount',
                           'L4_UnassessedCount', 'L4_AssessmentConfidence', 'L4_ConfidenceCategory', 'Level_5',
                           'L5_FinalResistance', 'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AssessmentConfidence',
                           'L5_ConfidenceCategory']]


# Review the newly developed MasterFrame, and export to a .csv format file. To export the data, utilise the export code
# which is stored as a comment (#) - ensure that you select an appropriate file path when completing this stage.


#       Export MasterFrame in CSV format  - Offshore Only
MasterFrame.to_csv('INSERT FILEPATH HERE \INSERT FILE NAME HERE.csv',
                   sep=',')

#       Export MasterFrame in CSV format - All - Inshore & Offshore
MasterFrame.to_csv('INSERT FILEPATH HERE \INSERT FILE NAME HERE.csv',
                  sep=',')
